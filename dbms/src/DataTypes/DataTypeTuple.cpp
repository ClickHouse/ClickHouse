#include <Columns/ColumnTuple.h>
#include <Columns/ColumnConst.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataTypes/DataTypeTuple.h>

#include <ext/map.h>
#include <ext/enumerate.h>
#include <ext/range.h>


namespace DB
{

std::string DataTypeTuple::getName() const
{
    std::stringstream s;

    s << "Tuple(";
    for (DataTypes::const_iterator it = elems.begin(); it != elems.end(); ++it)
        s << (it == elems.begin() ? "" : ", ") << (*it)->getName();
    s << ")";

    return s.str();
}


static inline IColumn & extractElementColumn(IColumn & column, size_t idx)
{
    return *static_cast<ColumnTuple &>(column).getData().getByPosition(idx).column.get();
}

static inline const IColumn & extractElementColumn(const IColumn & column, size_t idx)
{
    return *static_cast<const ColumnTuple &>(column).getData().getByPosition(idx).column.get();
}


void DataTypeTuple::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const auto & tuple = get<const Tuple &>(field).t;
    for (const auto & idx_elem : ext::enumerate(elems))
        idx_elem.second->serializeBinary(tuple[idx_elem.first], ostr);
}

void DataTypeTuple::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    const size_t size = elems.size();
    field = Tuple(TupleBackend(size));
    TupleBackend & tuple = get<Tuple &>(field).t;
    for (const auto i : ext::range(0, size))
        elems[i]->deserializeBinary(tuple[i], istr);
}

void DataTypeTuple::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    for (const auto & idx_elem : ext::enumerate(elems))
        idx_elem.second->serializeBinary(extractElementColumn(column, idx_elem.first), row_num, ostr);
}


template <typename F>
static void deserializeSafe(const DataTypes & elems, IColumn & column, ReadBuffer & istr, F && impl)
{
    /// We use the assumption that tuples of zero size do not exist.
    size_t old_size = extractElementColumn(column, 0).size();

    try
    {
        impl();
    }
    catch (...)
    {
        for (const auto & i : ext::range(0, ext::size(elems)))
        {
            auto & element_column = extractElementColumn(column, i);
            if (element_column.size() > old_size)
                element_column.popBack(1);
        }

        throw;
    }
}


void DataTypeTuple::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    deserializeSafe(elems, column, istr, [&]
    {
        for (const auto & i : ext::range(0, ext::size(elems)))
            elems[i]->deserializeBinary(extractElementColumn(column, i), istr);
    });
}

void DataTypeTuple::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeChar('(', ostr);
    for (const auto i : ext::range(0, ext::size(elems)))
    {
        if (i != 0)
            writeChar(',', ostr);
        elems[i]->serializeTextQuoted(extractElementColumn(column, i), row_num, ostr);
    }
    writeChar(')', ostr);
}

void DataTypeTuple::deserializeText(IColumn & column, ReadBuffer & istr) const
{
    const size_t size = elems.size();
    assertChar('(', istr);

    deserializeSafe(elems, column, istr, [&]
    {
        for (const auto i : ext::range(0, size))
        {
            skipWhitespaceIfAny(istr);
            if (i != 0)
                assertChar(',', istr);
            elems[i]->deserializeTextQuoted(extractElementColumn(column, i), istr);
        }
    });

    skipWhitespaceIfAny(istr);
    assertChar(')', istr);
}

void DataTypeTuple::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    serializeText(column, row_num, ostr);
}

void DataTypeTuple::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
    deserializeText(column, istr);
}

void DataTypeTuple::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    serializeText(column, row_num, ostr);
}

void DataTypeTuple::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
    deserializeText(column, istr);
}

void DataTypeTuple::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON & settings) const
{
    writeChar('[', ostr);
    for (const auto i : ext::range(0, ext::size(elems)))
    {
        if (i != 0)
            writeChar(',', ostr);
        elems[i]->serializeTextJSON(extractElementColumn(column, i), row_num, ostr, settings);
    }
    writeChar(']', ostr);
}

void DataTypeTuple::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
    const size_t size = elems.size();
    assertChar('[', istr);

    deserializeSafe(elems, column, istr, [&]
    {
        for (const auto i : ext::range(0, size))
        {
            skipWhitespaceIfAny(istr);
            if (i != 0)
                assertChar(',', istr);
            elems[i]->deserializeTextJSON(extractElementColumn(column, i), istr);
        }
    });

    skipWhitespaceIfAny(istr);
    assertChar(']', istr);
}

void DataTypeTuple::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeCString("<tuple>", ostr);
    for (const auto i : ext::range(0, ext::size(elems)))
    {
        writeCString("<elem>", ostr);
        elems[i]->serializeTextXML(extractElementColumn(column, i), row_num, ostr);
        writeCString("</elem>", ostr);
    }
    writeCString("</tuple>", ostr);
}

void DataTypeTuple::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    for (const auto i : ext::range(0, ext::size(elems)))
    {
        if (i != 0)
            writeChar(',', ostr);
        elems[i]->serializeTextCSV(extractElementColumn(column, i), row_num, ostr);
    }
}

void DataTypeTuple::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const
{
    deserializeSafe(elems, column, istr, [&]
    {
        const size_t size = elems.size();
        for (const auto i : ext::range(0, size))
        {
            if (i != 0)
            {
                skipWhitespaceIfAny(istr);
                assertChar(delimiter, istr);
                skipWhitespaceIfAny(istr);
            }
            elems[i]->deserializeTextCSV(extractElementColumn(column, i), istr, delimiter);
        }
    });
}

void DataTypeTuple::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const ColumnTuple & real_column = static_cast<const ColumnTuple &>(column);
    for (size_t i = 0, size = elems.size(); i < size; ++i)
        NativeBlockOutputStream::writeData(*elems[i], real_column.getData().safeGetByPosition(i).column, ostr, offset, limit);
}

void DataTypeTuple::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
    ColumnTuple & real_column = static_cast<ColumnTuple &>(column);
    for (size_t i = 0, size = elems.size(); i < size; ++i)
        NativeBlockInputStream::readData(*elems[i], *real_column.getData().safeGetByPosition(i).column, istr, limit);
}

ColumnPtr DataTypeTuple::createColumn() const
{
    Block tuple_block;
    for (size_t i = 0, size = elems.size(); i < size; ++i)
    {
        ColumnWithTypeAndName col;
        col.column = elems[i]->createColumn();
        col.type = elems[i]->clone();
        tuple_block.insert(std::move(col));
    }
    return std::make_shared<ColumnTuple>(tuple_block);
}

ColumnPtr DataTypeTuple::createConstColumn(size_t size, const Field & field) const
{
    return std::make_shared<ColumnConstTuple>(size, get<const Tuple &>(field), std::make_shared<DataTypeTuple>(elems));
}

Field DataTypeTuple::getDefault() const
{
    return Tuple(ext::map<TupleBackend>(elems, [] (const DataTypePtr & elem) { return elem->getDefault(); }));
}

}
