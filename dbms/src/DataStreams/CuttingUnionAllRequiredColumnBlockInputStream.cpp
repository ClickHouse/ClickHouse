#include <DataStreams/CuttingUnionAllRequiredColumnBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNION_ALL_RESULT_STRUCTURES_MISMATCH;
}

CuttingUnionAllRequiredColumnBlockInputStream::CuttingUnionAllRequiredColumnBlockInputStream(
    const BlockInputStreamPtr & input, const Names & required_columns)
{
    children.emplace_back(input);
    Block sample_block = input->getHeader();

    header = sample_block;

    if (!required_columns.empty())
    {
        need_cutting = true;
        Names columns_name = sample_block.getNames();

        Block cutting_header;
        cutting_pos.reserve(required_columns.size());
        for (size_t required_num = 0, size = required_columns.size(); required_num < size; ++required_num)
        {
            for (size_t column_num = 0, column_size = columns_name.size(); column_num < column_size; ++column_num)
            {
                if (required_columns.at(required_num) == columns_name.at(column_num))
                {
                    cutting_pos.emplace_back(column_num);
                    cutting_header.insert(sample_block.getByPosition(column_num).cloneEmpty());
                    break;
                }
            }
        }

        if (cutting_header.columns() != required_columns.size())
            throw Exception("Different number of columns in UNION ALL elements", ErrorCodes::UNION_ALL_RESULT_STRUCTURES_MISMATCH);

        header = cutting_header;
    }
}

Block CuttingUnionAllRequiredColumnBlockInputStream::readImpl()
{
    Block block = children.back()->read();

    if (need_cutting)
    {
        Block new_block;

        if (block)
        {
            for (size_t cut_num = 0, size = cutting_pos.size(); cut_num < size; ++cut_num)
            {
                ColumnWithTypeAndName column = block.getByPosition(cutting_pos[cut_num]);
                new_block.insert(std::move(column));
            }

            return new_block;
        }
    }

    return block;
}

}
