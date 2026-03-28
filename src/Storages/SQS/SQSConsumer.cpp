#include <Storages/SQS/SQSConsumer.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

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
}

namespace
{
    std::atomic<UInt32> consumer_id_counter{0};
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
    : consumer_id(consumer_id_counter.fetch_add(1))
    , queue_url(queue_url_)
    , client(client_)
    , max_messages_per_receive(max_messages_per_receive_)
    , visibility_timeout(visibility_timeout_)
    , wait_time_seconds(wait_time_seconds_)
    , dead_letter_queue_url(dead_letter_queue_url_)
    , max_receive_count(max_receive_count_)
    , queue(internal_queue_size_)
    , log(getLogger("SQSConsumer(" + std::to_string(consumer_id) + ")"))
{
    if (queue_url.empty())
        throw Exception(ErrorCodes::CANNOT_CONNECT_SQS, "SQS queue URL cannot be empty");
}

SQSConsumer::~SQSConsumer()
{
    is_running = false;
    queue.clear();
}

Aws::SQS::Model::ReceiveMessageRequest SQSConsumer::makeReceiveMessageRequest() const
{
    Aws::SQS::Model::ReceiveMessageRequest request;
    request.SetQueueUrl(queue_url);
    request.SetMaxNumberOfMessages(static_cast<int>(max_messages_per_receive));
    request.AddMessageSystemAttributeNames(Aws::SQS::Model::MessageSystemAttributeName::All);
    if (visibility_timeout > 0)
        request.SetVisibilityTimeout(visibility_timeout);
    if (wait_time_seconds > 0)
        request.SetWaitTimeSeconds(wait_time_seconds);
    return request;
}

bool SQSConsumer::receive()
{
    if (!is_running)
        return false;

    auto request = makeReceiveMessageRequest();

    auto outcome = client.ReceiveMessage(request);

    if (!outcome.IsSuccess())
    {
        const auto & error = outcome.GetError();
        if (error.GetErrorType() == Aws::SQS::SQSErrors::REQUEST_TIMEOUT)
            return false;

        throw Exception(
            ErrorCodes::CANNOT_CONNECT_SQS,
            "Failed to receive messages from SQS queue {}: {} ({})",
            queue_url,
            error.GetMessage(),
            error.GetExceptionName());
    }

    const auto & messages = outcome.GetResult().GetMessages();
    if (messages.empty())
        return false;

    bool pushed_any = false;
    for (const auto & msg : messages)
    {
        Message m;
        m.data = msg.GetBody();
        m.receipt_handle = msg.GetReceiptHandle();
        m.message_id = msg.GetMessageId();

        for (const auto & [attr_name, attr_value] : msg.GetAttributes())
        {
            switch (attr_name)
            {
                case Aws::SQS::Model::MessageSystemAttributeName::ApproximateReceiveCount:
                    try { m.receive_count = std::stoul(attr_value); } catch (...) { m.receive_count = 1; }
                    break;
                case Aws::SQS::Model::MessageSystemAttributeName::SentTimestamp:
                    try { m.sent_timestamp = std::stoull(attr_value) / 1000; } catch (...) { m.sent_timestamp = 0; }
                    break;
                case Aws::SQS::Model::MessageSystemAttributeName::MessageGroupId:
                    m.message_group_id = attr_value;
                    break;
                case Aws::SQS::Model::MessageSystemAttributeName::MessageDeduplicationId:
                    m.message_deduplication_id = attr_value;
                    break;
                case Aws::SQS::Model::MessageSystemAttributeName::SequenceNumber:
                    try { m.sequence_number = std::stoul(attr_value); } catch (...) { m.sequence_number = 0; }
                    break;
                default:
                    break;
            }
        }

        if (!dead_letter_queue_url.empty() && m.receive_count >= max_receive_count)
        {
            LOG_INFO(log, "Message {} exceeded max receive count ({}/{}), moving to DLQ",
                m.message_id, m.receive_count, max_receive_count);
            moveMessageToDLQ(m);
            continue;
        }

        if (queue.tryPush(m))
            pushed_any = true;
    }

    return pushed_any;
}

std::optional<SQSConsumer::Message> SQSConsumer::getMessage()
{
    if (!is_running)
        return std::nullopt;

    Message message;
    if (!queue.tryPop(message))
        return std::nullopt;

    return message;
}

void SQSConsumer::deleteMessage(const String & receipt_handle)
{
    if (!is_running || receipt_handle.empty())
        return;

    Aws::SQS::Model::DeleteMessageRequest request;
    request.SetQueueUrl(queue_url);
    request.SetReceiptHandle(receipt_handle);

    auto outcome = client.DeleteMessage(request);
    if (!outcome.IsSuccess())
    {
        const auto & error = outcome.GetError();
        LOG_WARNING(log, "Failed to delete message from SQS queue {}: {} ({})",
            queue_url, error.GetMessage(), error.GetExceptionName());
    }
}

void SQSConsumer::moveMessageToDLQ(const Message & message)
{
    if (dead_letter_queue_url.empty())
        return;

    try
    {
        Aws::SQS::Model::SendMessageRequest request;
        request.SetQueueUrl(dead_letter_queue_url);
        request.SetMessageBody(message.data);

        Aws::Map<Aws::String, Aws::SQS::Model::MessageAttributeValue> attributes;

        auto make_str_attr = [](const String & value)
        {
            Aws::SQS::Model::MessageAttributeValue attr;
            attr.SetDataType("String");
            attr.SetStringValue(value);
            return attr;
        };

        auto make_num_attr = [](const String & value)
        {
            Aws::SQS::Model::MessageAttributeValue attr;
            attr.SetDataType("Number");
            attr.SetStringValue(value);
            return attr;
        };

        attributes["OriginalMessageId"] = make_str_attr(message.message_id);
        attributes["OriginalReceiveCount"] = make_num_attr(std::to_string(message.receive_count));

        if (message.sent_timestamp > 0)
            attributes["OriginalSentTimestamp"] = make_num_attr(std::to_string(message.sent_timestamp));

        if (!message.message_group_id.empty())
        {
            request.SetMessageGroupId(message.message_group_id);
            attributes["OriginalMessageGroupId"] = make_str_attr(message.message_group_id);
        }

        if (!message.message_deduplication_id.empty())
        {
            request.SetMessageDeduplicationId(message.message_deduplication_id);
            attributes["OriginalMessageDeduplicationId"] = make_str_attr(message.message_deduplication_id);
        }

        request.SetMessageAttributes(attributes);

        auto outcome = client.SendMessage(request);
        if (!outcome.IsSuccess())
        {
            const auto & error = outcome.GetError();
            LOG_ERROR(log, "Failed to send message to DLQ {}: {} ({})",
                dead_letter_queue_url, error.GetMessage(), error.GetExceptionName());
            return;
        }

        deleteMessage(message.receipt_handle);
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "Unexpected error when moving message {} to DLQ: {}", message.message_id, e.what());
    }
}

void SQSConsumer::stop()
{
    is_running = false;
}

}

#endif // USE_AWS_SQS
