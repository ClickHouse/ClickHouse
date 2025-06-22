#pragma once

#include "config.h"


// #include <Columns/IColumn_fwd.h>
// #include <Core/Names.h>
// #include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>

#if USE_PROTOBUF
#    include <google/protobuf/descriptor.h>
#    include <google/protobuf/descriptor.pb.h>


namespace DB
{

class OneOfColumnsFiller
{
public:
    using FieldDescriptor = google::protobuf::FieldDescriptor;
    using MessageDescriptor = google::protobuf::Descriptor;
    using FieldTypeId = google::protobuf::FieldDescriptor::Type;
    using OneofDescriptor = google::protobuf::OneofDescriptor;


    void add(size_t row_num, const FieldDescriptor * message_descriptor, int field_tag);
    void list();
    void breadcrumbRegister(std::string_view column_name, void * serializer_ptr);

    ~OneOfColumnsFiller();
    OneOfColumnsFiller() = default;
    OneOfColumnsFiller(const OneOfColumnsFiller &) = delete;

private:
    struct Holder
    {
        size_t row_num;
        // String oneof_name;

        // info.field_descriptor    containing_type
        const FieldDescriptor * message_descriptor;

        int field_tag;
    };
    std::vector<Holder> holder;
    std::string descriptor_to_name(const FieldDescriptor * field_descriptor);
    const FieldDescriptor * stepUp(const FieldDescriptor * field_descriptor);

    std::unordered_map<const FieldDescriptor *, std::string> descriptor_to_name_cache;

    std::vector<std::pair<std::string, void *>> breadcrumbs;
};

}

#endif
