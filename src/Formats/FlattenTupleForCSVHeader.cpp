#include <Formats/FlattenTupleForCSVHeader.h>

#include <Core/Block.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace
{
void flattenField(const String & name, const DataTypePtr & type, Names & names, Names & type_names)
{
    /// A non-null Nullable(Tuple(...)) value serializes through the nested tuple
    /// (SerializationNullable::serializeTextCSV delegates to it), so it is flattened into separate
    /// columns; the header must flatten the inner tuple to match. Scalar Nullable types serialize as
    /// one field and stay one cell.
    const IDataType * unwrapped = type.get();
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(unwrapped))
        unwrapped = nullable_type->getNestedType().get();

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(unwrapped))
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
