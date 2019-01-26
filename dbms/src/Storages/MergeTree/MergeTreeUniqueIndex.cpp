#include <Storages/MergeTree/MergeTreeUniqueIndex.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

MergeTreeUniqueGranule::MergeTreeUniqueGranule(const MergeTreeUniqueIndex & index)
        : MergeTreeIndexGranule(), index(index), block()
{
}

void MergeTreeUniqueGranule::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(
                "Attempt to write empty unique index `" + index.name + "`", ErrorCodes::LOGICAL_ERROR);
    Poco::Logger * log = &Poco::Logger::get("unique_idx");

    LOG_DEBUG(log, "serializeBinary Granule");

    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        const DataTypePtr & type = index.data_types[i];

        type->serializeBinary(block.getByPosition(i).column, ostr);
    }
}

void MergeTreeUniqueGranule::deserializeBinary(ReadBuffer & istr)
{
    Poco::Logger * log = &Poco::Logger::get("unique_idx");

    LOG_DEBUG(log, "deserializeBinary Granule");
    block.clear();
    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        const DataTypePtr & type = index.data_types[i];

        auto new_column = type->createColumn();
        type->deserializeBinary(*new_column, istr);

        block.insert(ColumnWithTypeAndName(new_column->getPtr(), type, index.columns[i]));
    }
}

String MergeTreeUniqueGranule::toString() const
{
    String res = "unique granule:\n";

    for (size_t i = 0; i < block.columns(); ++i)
    {
        const auto & column = block.getByPosition(i);
        res += column.name;
        res += " [";
        for (size_t j = 0; j < column.column->size(); ++j)
        {
            if (j != 0)
                res += ", ";
            Field field;
            column.column->get(j, field);
            res += applyVisitor(FieldVisitorToString(), field);
        }
        res += "]\n";
    }

    return res;
}

void MergeTreeUniqueGranule::update(const Block & new_block, size_t * pos, size_t limit)
{
    Poco::Logger * log = &Poco::Logger::get("unique_idx");

    LOG_DEBUG(log, "update Granule " << new_block.columns()
                                     << " pos: "<< *pos << " limit: " << limit << " rows: " << new_block.rows());

    size_t cur = 0;
    size_t block_size = new_block.getByPosition(0).column->size();

    if (!block.columns())
    {
        for (size_t i = 0; i < index.columns.size(); ++i)
        {
            const DataTypePtr & type = index.data_types[i];
            block.insert(ColumnWithTypeAndName(type->createColumn(), type, index.columns[i]));
        }
    }

    for (cur = 0; cur < limit && cur + *pos < block_size; ++cur)
    {
        Field field;
        column->get(cur + *pos, field);
        LOG_DEBUG(log, "upd:: " << applyVisitor(FieldVisitorToString(), field));
        if (parallelogram.size() <= i)
        {
            LOG_DEBUG(log, "emplaced");
            parallelogram.emplace_back(field, true, field, true);
        }
        else
        {
            parallelogram[i].left = std::min(parallelogram[i].left, field);
            parallelogram[i].right = std::max(parallelogram[i].right, field);
        }
    }
    *pos += cur;

    LOG_DEBUG(log, "updated rows_read: " << rows_read);

};

}