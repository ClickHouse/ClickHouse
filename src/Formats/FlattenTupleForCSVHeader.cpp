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
    /// Only a bare Tuple is flattened. A Nullable(Tuple(...)) is serialized as a single CSV field
    /// (SerializationNullable::serializeTextCSV writes it as one quoted value, matching the single
    /// \\N cell of a NULL row), so its header must stay one cell too. Do not unwrap Nullable here.
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        const auto & elems = tuple_type->getElements();
        /// An empty tuple still occupies exactly one field: the value serializer writes nothing
        /// between the surrounding delimiters. Emit one header cell so widths agree.
        if (elems.empty())
        {
            names.push_back(name);
            type_names.push_back(type->getName());
            return;
        }

        const auto & elem_names = tuple_type->getElementNames();
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
