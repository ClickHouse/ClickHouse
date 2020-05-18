#include <iostream>
#include <iomanip>
#include <pcg_random.hpp>

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Core/SortDescription.h>

#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/PartialSortingBlockInputStream.h>
#include <DataStreams/FinishSortingBlockInputStream.h>

#include <Interpreters/sortBlock.h>


using namespace DB;

namespace DB
{
    namespace ErrorCodes
    {
        extern const int LOGICAL_ERROR;
    }
}


int main(int argc, char ** argv)
{
    pcg64 rng;

    try
    {
        size_t m = argc >= 2 ? std::stol(argv[1]) : 2;
        size_t n = argc >= 3 ? std::stol(argv[2]) : 10;

        Blocks blocks;
        for (size_t t = 0; t < m; ++t)
        {
            Block block;
            for (size_t i = 0; i < 2; ++i)
            {
                ColumnWithTypeAndName column;
                column.name = "col" + std::to_string(i + 1);
                column.type = std::make_shared<DataTypeInt32>();

                auto col = ColumnInt32::create();
                auto & vec = col->getData();
                vec.resize(n);

                for (size_t j = 0; j < n; ++j)
                    vec[j] = rng() % 10;

                column.column = std::move(col);
                block.insert(column);
            }
            blocks.push_back(block);
        }

        SortDescription sort_descr;
        sort_descr.emplace_back("col1", 1, 1);

        for (auto & block : blocks)
            sortBlock(block, sort_descr);

        BlockInputStreamPtr stream = std::make_shared<MergeSortingBlocksBlockInputStream>(blocks, sort_descr, n);

        SortDescription sort_descr_final;
        sort_descr_final.emplace_back("col1", 1, 1);
        sort_descr_final.emplace_back("col2", 1, 1);

        stream = std::make_shared<FinishSortingBlockInputStream>(stream, sort_descr, sort_descr_final, n, 0);

        {
            Stopwatch stopwatch;
            stopwatch.start();

            Block res_block = blocks[0].cloneEmpty();

            while (Block block = stream->read())
            {
                for (size_t i = 0; i < block.columns(); ++i)
                {
                    MutableColumnPtr ptr = IColumn::mutate(std::move(res_block.getByPosition(i).column));
                    ptr->insertRangeFrom(*block.getByPosition(i).column.get(), 0, block.rows());
                }
            }

            if (res_block.rows() != n * m)
                throw Exception("Result block size mismatch", ErrorCodes::LOGICAL_ERROR);

            const auto & columns = res_block.getColumns();

            for (size_t i = 1; i < res_block.rows(); ++i)
                for (const auto & col : columns)
                {
                    int res = col->compareAt(i - 1, i, *col, 1);
                    if (res < 0)
                        break;
                    else if (res > 0)
                        throw Exception("Result stream not sorted", ErrorCodes::LOGICAL_ERROR);
                }

            stopwatch.stop();
            std::cout << std::fixed << std::setprecision(2)
                << "Elapsed " << stopwatch.elapsedSeconds() << " sec."
                << ", " << n / stopwatch.elapsedSeconds() << " rows/sec."
                << std::endl;
        }
    }
    catch (const Exception & e)
    {
        std::cerr << e.displayText() << std::endl;
        return -1;
    }

    return 0;
}
