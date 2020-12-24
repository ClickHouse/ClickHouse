#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_formats.h"
#endif

#if USE_PROTOBUF
#    include <memory>
#    include <unordered_map>
#    include <vector>
#    include <Core/Types.h>
#    include <boost/blank.hpp>
#    include <google/protobuf/descriptor.h>
#    include <google/protobuf/descriptor.pb.h>

namespace google
{
namespace protobuf
{
    class Descriptor;
    class FieldDescriptor;
}
}


namespace DB
{
namespace ProtobufColumnMatcher
{
    struct DefaultTraits
    {
        using MessageData = boost::blank;
        using FieldData = boost::blank;
    };

    template <typename Traits = DefaultTraits>
    struct Message;

    /// Represents a field in a protobuf message.
    template <typename Traits = DefaultTraits>
    struct Field
    {
        const google::protobuf::FieldDescriptor * field_descriptor = nullptr;

        /// Same as field_descriptor->number().
        UInt32 field_number = 0;

        /// Index of a column; either 'column_index' or 'nested_message' is set.
        size_t column_index = -1;
        std::unique_ptr<Message<Traits>> nested_message;

        typename Traits::FieldData data;
    };

    /// Represents a protobuf message.
    template <typename Traits>
    struct Message
    {
        std::vector<Field<Traits>> fields;

        /// Points to the parent message if this is a nested message.
        Message * parent = nullptr;
        size_t index_in_parent = -1;

        typename Traits::MessageData data;
    };

    /// Utility function finding matching columns for each protobuf field.
    template <typename Traits = DefaultTraits>
    static std::unique_ptr<Message<Traits>> matchColumns(
        const std::vector<String> & column_names,
        const google::protobuf::Descriptor * message_type);

    template <typename Traits = DefaultTraits>
    static std::unique_ptr<Message<Traits>> matchColumns(
        const std::vector<String> & column_names,
        const google::protobuf::Descriptor * message_type,
        std::vector<const google::protobuf::FieldDescriptor *> & field_descriptors_without_match);

    namespace details
    {
        [[noreturn]] void throwNoCommonColumns();

        class ColumnNameMatcher
        {
        public:
            ColumnNameMatcher(const std::vector<String> & column_names);
            size_t findColumn(const String & field_name);

        private:
            std::unordered_map<String, size_t> column_name_to_index_map;
            std::vector<bool> column_usage;
        };

        template <typename Traits>
        std::unique_ptr<Message<Traits>> matchColumnsRecursive(
            ColumnNameMatcher & name_matcher,
            const google::protobuf::Descriptor * message_type,
            const String & field_name_prefix,
            std::vector<const google::protobuf::FieldDescriptor *> * field_descriptors_without_match)
        {
            auto message = std::make_unique<Message<Traits>>();
            for (int i = 0; i != message_type->field_count(); ++i)
            {
                const google::protobuf::FieldDescriptor * field_descriptor = message_type->field(i);
                if ((field_descriptor->type() == google::protobuf::FieldDescriptor::TYPE_MESSAGE)
                    || (field_descriptor->type() == google::protobuf::FieldDescriptor::TYPE_GROUP))
                {
                    auto nested_message = matchColumnsRecursive<Traits>(
                        name_matcher,
                        field_descriptor->message_type(),
                        field_name_prefix + field_descriptor->name() + ".",
                        field_descriptors_without_match);
                    if (nested_message)
                    {
                        message->fields.emplace_back();
                        auto & current_field = message->fields.back();
                        current_field.field_number = field_descriptor->number();
                        current_field.field_descriptor = field_descriptor;
                        current_field.nested_message = std::move(nested_message);
                        current_field.nested_message->parent = message.get();
                    }
                }
                else
                {
                    size_t column_index = name_matcher.findColumn(field_name_prefix + field_descriptor->name());
                    if (column_index == static_cast<size_t>(-1))
                    {
                        if (field_descriptors_without_match)
                            field_descriptors_without_match->emplace_back(field_descriptor);
                    }
                    else
                    {
                        message->fields.emplace_back();
                        auto & current_field = message->fields.back();
                        current_field.field_number = field_descriptor->number();
                        current_field.field_descriptor = field_descriptor;
                        current_field.column_index = column_index;
                    }
                }
            }

            if (message->fields.empty())
                return nullptr;

            // Columns should be sorted by field_number, it's necessary for writing protobufs and useful reading protobufs.
            std::sort(message->fields.begin(), message->fields.end(), [](const Field<Traits> & left, const Field<Traits> & right)
            {
                return left.field_number < right.field_number;
            });

            for (size_t i = 0; i != message->fields.size(); ++i)
            {
                auto & field = message->fields[i];
                if (field.nested_message)
                    field.nested_message->index_in_parent = i;
            }

            return message;
        }
    }

    template <typename Data>
    static std::unique_ptr<Message<Data>> matchColumnsImpl(
        const std::vector<String> & column_names,
        const google::protobuf::Descriptor * message_type,
        std::vector<const google::protobuf::FieldDescriptor *> * field_descriptors_without_match)
    {
        details::ColumnNameMatcher name_matcher(column_names);
        auto message = details::matchColumnsRecursive<Data>(name_matcher, message_type, "", field_descriptors_without_match);
        if (!message)
            details::throwNoCommonColumns();
        return message;
    }

    template <typename Data>
    static std::unique_ptr<Message<Data>> matchColumns(
        const std::vector<String> & column_names,
        const google::protobuf::Descriptor * message_type)
    {
        return matchColumnsImpl<Data>(column_names, message_type, nullptr);
    }

    template <typename Data>
    static std::unique_ptr<Message<Data>> matchColumns(
        const std::vector<String> & column_names,
        const google::protobuf::Descriptor * message_type,
        std::vector<const google::protobuf::FieldDescriptor *> & field_descriptors_without_match)
    {
        return matchColumnsImpl<Data>(column_names, message_type, &field_descriptors_without_match);
    }
}

}

#endif
