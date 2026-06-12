#include <Formats/FlattenTupleForCSVHeader.h>

#include <Core/Block.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace
{
void flattenField(const String & name, const DataTypePtr & type, Names & names, Names & type_names)
{
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        const auto & elem_names = tuple_type->getElementNames();
        const auto & elems = tuple_type->getElements();
        for (size_t i = 0; i < elems.size(); ++i)
            flattenField(name + "." + elem_names[i], elems[i], names, type_names);
    }
    else
    {
        names.push_back(name);
        type_names.push_back(type->getName());
    }
}
}

void getCSVHeaderNamesAndTypes(const Block & sample, bool flatten, Names & names, Names & type_names)
{
    for (const auto & elem : sample)
    {
        if (flatten)
            flattenField(elem.name, elem.type, names, type_names);
        else
        {
            names.push_back(elem.name);
            type_names.push_back(elem.type->getName());
        }
    }
}

}
