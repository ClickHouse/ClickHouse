#include <Formats/FlattenTupleForCSVHeader.h>

#include <Core/Block.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/isValidUTF8.h>
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

bool csvHeaderNamesMayProduceRawBytes(const Block & sample, bool flatten, bool with_names, bool with_types)
{
    if (!with_names && !with_types)
        return false;

    Names names;
    Names type_names;
    getCSVHeaderNamesAndTypes(sample, flatten, names, type_names);

    auto is_not_valid_utf8 = [](const std::string & s)
    {
        return !UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(s.data()), s.size());
    };

    if (with_names)
        for (const auto & name : names)
            if (is_not_valid_utf8(name))
                return true;

    if (with_types)
        for (const auto & type_name : type_names)
            if (is_not_valid_utf8(type_name))
                return true;

    return false;
}

}
