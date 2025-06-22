#include <Formats/OneOfColumnsFiller.h>
#include <boost/algorithm/string/join.hpp>

#if USE_PROTOBUF
#include <Common/logger_useful.h>

namespace DB
{
void OneOfColumnsFiller::add(size_t row_num, const FieldDescriptor * message_descriptor, int field_tag)
{
    holder.push_back({row_num, message_descriptor, field_tag});
}

void OneOfColumnsFiller::list()
{
    // for (const auto & a_row : holder)
    // {
    //     LOG_DEBUG(getLogger("OneOfColumnsFiller"), "row_num {}, oneof_name {}, field_tag {}", a_row.row_num, descriptor_to_name(a_row.message_descriptor), a_row.field_tag);
    // }
    for (const auto & breadcrumb  : breadcrumbs)
    {
        LOG_DEBUG(getLogger("OneOfColumnsFiller"), "column {}, serializer address {}", breadcrumb.first, breadcrumb.second);
    }
}

const OneOfColumnsFiller::FieldDescriptor * OneOfColumnsFiller::stepUp(const FieldDescriptor * field_descriptor)
{
    const auto * containing_type = field_descriptor->containing_type();
    const auto * fd = containing_type->FindFieldByNumber(field_descriptor->number());
    return fd;
}


std::string OneOfColumnsFiller::descriptor_to_name(const FieldDescriptor * field_descriptor)
{
    const auto it = descriptor_to_name_cache.find(field_descriptor);
    if (it != descriptor_to_name_cache.end())
    {
        return it->second;
    }

    // std::string full_name = field_descriptor->full_name();


    /*
Get the Descriptor of the field's containing type using field->containing_type().
Get the field's name using field->name().
Add the field's name to the path vector.
If the field is repeated (using field->is_repeated()), append "[]" to the path component.
If the field is a map (using field->is_map()), determine how to represent the key (e.g., by accessing a specific field if it's a message or using the key's string representation) and append [key] to the path.
Move to the parent descriptor by setting field = containing_type->FindFieldByNumber(field->number()). This assumes that the field numbers are consistent across parent messages. If not, you'll need to find the field by name using the parent descriptor.
Repeat until the containing_type is nullptr.
    */

    std::list<std::string> path;
    for (const auto * fd = field_descriptor; fd; fd = stepUp(fd))
    {
        path.push_front(fd->name());
    }


    boost::algorithm::join(path, "_");

    // field_descriptor->containing_type()
    descriptor_to_name_cache.insert({field_descriptor, /* full_name */ boost::algorithm::join(path, "_")});


    return boost::algorithm::join(path, "_");
}

void OneOfColumnsFiller::breadcrumbRegister(std::string_view column_name, void * serializer_ptr)
{
    breadcrumbs.push_back({std::string(column_name), serializer_ptr});
}


OneOfColumnsFiller::~OneOfColumnsFiller()
{
    // list();
}



}


#endif
