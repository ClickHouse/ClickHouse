#include <Storages/SQS/SQSConsumer.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <atomic>

#include "config.h"

#if USE_AWS_SQS

#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/sqs/model/ReceiveMessageResult.h>
#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/sqs/model/SendMessageRequest.h>
#include <aws/sqs/SQSErrors.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_SQS;
    extern const int TIMEOUT_EXCEEDED;
}

namespace 
{
    std::atomic<UInt32> consumer_counter;
}

SQSConsumer::SQSConsumer(
    const String & queue_url_,
    const Aws::SQS::SQSClient & client_,
    size_t max_messages_per_receive_,
    int visibility_timeout_,
    int wait_time_seconds_,
    const String & dead_letter_queue_url_,
    size_t max_receive_count_,
    size_t internal_queue_size_)
    : consumer_id(consumer_counter.fetch_add(1))
    , queue_url(queue_url_)
    , client(client_)
    , max_messages_per_receive(max_messages_per_receive_)
    , visibility_timeout(visibility_timeout_)
    , wait_time_seconds(wait_time_seconds_)
    , dead_letter_queue_url(dead_letter_queue_url_)
    , max_receive_count(max_receive_count_)
    , queue(internal_queue_size_)
    , is_running(true)
{
    if (queue_url.empty())
        throw Exception(ErrorCodes::CANNOT_CONNECT_SQS, "SQS queue URL cannot be empty");
}

SQSConsumer::~SQSConsumer()
{
    is_running = false;
    queue.clear();
}

Aws::SQS::Model::ReceiveMessageRequest SQSConsumer::makeReceiveMessageRequest() {
    Aws::SQS::Model::ReceiveMessageRequest request;
    request.SetQueueUrl(queue_url);
    request.SetMaxNumberOfMessages(static_cast<int>(max_messages_per_receive));
    request.AddAttributeNames(Aws::SQS::Model::QueueAttributeName::All);
    if (visibility_timeout > 0)
    {
        request.SetVisibilityTimeout(visibility_timeout);
    }
    if (wait_time_seconds > 0)
    {
        request.SetWaitTimeSeconds(wait_time_seconds);
    }

    return request;
}

bool SQSConsumer::receive()
{
    if (!is_running)
        return false;

    auto request = makeReceiveMessageRequest();
    
    try
    {
        auto outcome = client.ReceiveMessage(request);
        
        if (!outcome.IsSuccess())
        {
            const auto & error = outcome.GetError();
            if (error.GetErrorType() == Aws::SQS::SQSErrors::REQUEST_TIMEOUT)
            {
                return false;
            }
            
            LOG_ERROR(&Poco::Logger::get("SQSConsumer"), 
                "Error receiving message from SQS queue ({}): {} ({})", 
                queue_url, error.GetMessage(), error.GetExceptionName());
            
            throw Exception(
                ErrorCodes::CANNOT_CONNECT_SQS,
                "Failed to receive message from SQS queue ({}): {} ({})",
                queue_url,
                error.GetMessage(),
                error.GetExceptionName());
        }
        
        const auto & result = outcome.GetResult();
        const auto & messages = result.GetMessages();
        
        if (messages.empty())
            return false;
        
        bool success = false;
        for (const auto & message : messages)
        {
            Message msg;
            msg.data = message.GetBody();
            msg.receipt_handle = message.GetReceiptHandle();
            msg.message_id = message.GetMessageId();
            
            // Process system message attributes
            const auto & attributes = message.GetAttributes();
            for (const auto & attr_pair : attributes)
            {
                const auto & attr_name = attr_pair.first;
                const auto & attr_value = attr_pair.second;
                                
                switch (attr_name)
                {
                    case Aws::SQS::Model::MessageSystemAttributeName::ApproximateReceiveCount:
                        try {
                            msg.receive_count = std::stoul(attr_value);
                        } catch (...) {
                            msg.receive_count = 1; // Default 1
                        }
                        break;
                    case Aws::SQS::Model::MessageSystemAttributeName::SentTimestamp:
                        try {
                            // Convert from milliseconds to seconds for DateTime64
                            UInt64 timestamp_ms = std::stoull(attr_value);
                            
                            msg.sent_timestamp = timestamp_ms / 1000;
                        } catch (...) {
                            msg.sent_timestamp = 0;
                        }
                        break;
                    case Aws::SQS::Model::MessageSystemAttributeName::MessageGroupId:
                        msg.message_group_id = attr_value;
                        break;
                    case Aws::SQS::Model::MessageSystemAttributeName::MessageDeduplicationId:
                        msg.message_deduplication_id = attr_value;
                        break;
                    case Aws::SQS::Model::MessageSystemAttributeName::SequenceNumber:
                        try {
                            msg.sequence_number = std::stoul(attr_value);
                        } catch (...) {
                            msg.sequence_number = 0;
                        }
                        break;
                    default:
                        // Ignore unknown attributes
                        break;
                }
            }
            
            // Check the number of attempts for DLQ
            if (!dead_letter_queue_url.empty() && msg.receive_count >= max_receive_count)
            {
                LOG_INFO(&Poco::Logger::get("SQSConsumer"), 
                    "ConsumerID: {}, Message with ID {} exceeded the maximum number of attempts ({}/{}), moving to DLQ", 
                    consumer_id, msg.message_id, msg.receive_count, max_receive_count);
                
                moveMessageToDLQ(msg);
                continue; // Skip further processing of this message
            }
            
            if (queue.tryPush(msg))
            {
                success = true;
            }
        }
        return success;
    }
    catch (const Poco::Exception & e)
    {
        LOG_WARNING(&Poco::Logger::get("SQSConsumer"), 
            "Network error when working with SQS ({}): {}", queue_url, e.displayText());
        
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_SQS,
            "Network error when receiving message from SQS queue ({}): {}",
            queue_url,
            e.displayText());
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(&Poco::Logger::get("SQSConsumer"), 
            "Unexpected error when receiving messages from SQS ({}): {}", queue_url, e.what());
        
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_SQS,
            "Unexpected error when receiving message from SQS queue ({}): {}",
            queue_url,
            e.what());
    }
}

std::optional<SQSConsumer::Message> SQSConsumer::getMessage()
{
    if (!is_running)
        return std::nullopt;

    Message message;
    bool success = queue.tryPop(message);
    
    if (!success)
        return std::nullopt;
    
    return message;
}

void SQSConsumer::deleteMessage(const String & receipt_handle)
{
    if (!is_running)
        return;

    if (receipt_handle.empty())
        return;
    
    Aws::SQS::Model::DeleteMessageRequest request;
    request.SetQueueUrl(queue_url);
    request.SetReceiptHandle(receipt_handle);
    
    auto outcome = client.DeleteMessage(request);

    if (!outcome.IsSuccess())
    {
        const auto & error = outcome.GetError();
        LOG_WARNING(&Poco::Logger::get("SQSConsumer"), 
            "Failed to delete message from SQS queue ({}): {} ({})",
            queue_url,
            error.GetMessage(),
            error.GetExceptionName());
    }
}

void SQSConsumer::moveMessageToDLQ(const Message & message)
{
    if (dead_letter_queue_url.empty())
        return;
    
    try 
    {
        LOG_INFO(&Poco::Logger::get("SQSConsumer"), 
            "Moving message to DLQ, ID: {}, attempts: {}/{}, DLQ URL: {}", 
            message.message_id, message.receive_count, max_receive_count, dead_letter_queue_url);
        
        // Create a request to send a message to the DLQ
        Aws::SQS::Model::SendMessageRequest request;
        request.SetQueueUrl(dead_letter_queue_url);
        request.SetMessageBody(message.data);
        
        // Copy message attributes to save metadata
        Aws::Map<Aws::String, Aws::SQS::Model::MessageAttributeValue> attributes;
        
        // Add information about the original message
        Aws::SQS::Model::MessageAttributeValue original_id_attr;
        original_id_attr.SetDataType("String");
        original_id_attr.SetStringValue(message.message_id.c_str());
        attributes["OriginalMessageId"] = original_id_attr;
        
        // Add information about the number of attempts to process
        Aws::SQS::Model::MessageAttributeValue receive_count_attr;
        receive_count_attr.SetDataType("Number");
        receive_count_attr.SetStringValue(std::to_string(message.receive_count));
        attributes["OriginalReceiveCount"] = receive_count_attr;
        
        // Add information about the time of sending the original message
        if (message.sent_timestamp > 0)
        {
            Aws::SQS::Model::MessageAttributeValue sent_timestamp_attr;
            sent_timestamp_attr.SetDataType("Number");
            sent_timestamp_attr.SetStringValue(std::to_string(message.sent_timestamp));
            attributes["OriginalSentTimestamp"] = sent_timestamp_attr;
        }
        
        // For FIFO queues, copy the group and deduplication IDs
        if (!message.message_group_id.empty())
        {
            request.SetMessageGroupId(message.message_group_id);
            
            Aws::SQS::Model::MessageAttributeValue group_id_attr;
            group_id_attr.SetDataType("String");
            group_id_attr.SetStringValue(message.message_group_id.c_str());
            attributes["OriginalMessageGroupId"] = group_id_attr;
        }
        
        if (!message.message_deduplication_id.empty())
        {
            request.SetMessageDeduplicationId(message.message_deduplication_id);
            
            Aws::SQS::Model::MessageAttributeValue dedup_id_attr;
            dedup_id_attr.SetDataType("String");
            dedup_id_attr.SetStringValue(message.message_deduplication_id.c_str());
            attributes["OriginalMessageDeduplicationId"] = dedup_id_attr;
        }
        
        // Set attributes in the request
        request.SetMessageAttributes(attributes);
        
        // Send message to DLQ
        auto outcome = client.SendMessage(request);
        
        if (!outcome.IsSuccess())
        {
            const auto & error = outcome.GetError();
            LOG_ERROR(&Poco::Logger::get("SQSConsumer"), 
                "Error sending message to DLQ ({}): {} ({})", 
                dead_letter_queue_url, error.GetMessage(), error.GetExceptionName());
        }
        else
        {
            deleteMessage(message.receipt_handle);
        }
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(&Poco::Logger::get("SQSConsumer"), 
            "Unexpected error when moving message to DLQ: {}", e.what());
    }
}

void SQSConsumer::stop()
{
    is_running = false;
}

}

#endif // USE_AWS_SQS
