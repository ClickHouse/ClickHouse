#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <DataStreams/EarlyWindowBlockInputStream.h>
#include <Common/ClickHouseRevision.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>


namespace ProfileEvents
{
    extern const Event ExternalAggregationMerge;
}


namespace DB
{


EarlyWindowBlockInputStream::EarlyWindowBlockInputStream(const BlockInputStreamPtr & input, const std::vector<Aggregator::Params> & params_vec_)
    : params_vec(params_vec_), executed(false)
{
    for (auto & params : params_vec)
        aggregators.push_back(new Aggregator(params));

    children.push_back(input);
}


EarlyWindowBlockInputStream::~EarlyWindowBlockInputStream()
{
    for (auto aggregator : aggregators)
        delete aggregator;
}


Block EarlyWindowBlockInputStream::getHeader() const
{
    Block res= children.back()->getHeader();

    for (auto aggregator : aggregators)
        for (size_t i = 0; i < aggregator->params.aggregates_size; ++i)
            res.insert({ aggregator->params.aggregates[0].function->getReturnType(), aggregator->params.aggregates[0].column_name });

    return res;
}


Block EarlyWindowBlockInputStream::readImpl()
{
    if (!executed)
    {
        executed = true;
        std::vector<AggregatedDataVariantsPtr> data_variants_vec;
        for (size_t i = 0; i < aggregators.size(); ++i)
            data_variants_vec.push_back(std::make_shared<AggregatedDataVariants>());

        Blocks blocks;
        std::vector<std::vector<PlacesPtr>> places_vecs;
        places_vecs.resize(aggregators.size());

        while (Block block = children.back()->read())
        {
            for (size_t i = 0; i < aggregators.size(); ++i)
            {
                auto & descr = aggregators[i]->params.aggregates[0];

                ColumnRawPtrs key_columns(1);   // TODO : Multiple Key-Columns
                Aggregator::AggregateColumns aggregate_columns(1);  // TODO : Multiple Aggregate-Columns
                bool no_more_keys = false;

                PlacesPtr places_ptr = aggregators[i]->executeOnBlockEarlyWindow(block, *(data_variants_vec[i]), key_columns, aggregate_columns, no_more_keys, descr);
                if (places_ptr == nullptr)
                    break;

                places_vecs[i].push_back(places_ptr);
            }
            blocks.push_back(block);
        }

        if (blocks.size() == 0)
            return {};

        Block res;
        MutableColumns columns(blocks[0].columns() + aggregators.size());

        for (size_t i = 0; i < blocks[0].columns(); ++i)
        {
            columns[i] = blocks[0].safeGetByPosition(i).type->createColumn();
            res.insert({blocks[0].safeGetByPosition(i).type, blocks[0].getByPosition(i).name});
        }

        for (size_t i = 0; i < aggregators.size(); ++i)
        {
            columns[blocks[0].columns() + i] = aggregators[i]->params.aggregates[0].function->getReturnType()->createColumn();
            res.insert({aggregators[i]->params.aggregates[0].function->getReturnType(), aggregators[i]->params.aggregates[0].column_name});
        }

        for (size_t i = 0; i < blocks.size(); ++i)
        {
            Block b = blocks[i];

            for (size_t col = 0; col < b.columns(); ++col)
            {
                auto from = b.getByPosition(col);

                columns[col]->insertRangeFrom(*from.column, 0, from.column->size());
            }
        }

        for (size_t i = 0; i < places_vecs.size(); ++i)
        {
            for (auto places = places_vecs[i].begin(); places != places_vecs[i].end(); ++places)
            {
                PlacesPtr pp = (*places);
                for (auto place = pp->begin(); place != pp->end(); ++place)
                    aggregators[i]->params.aggregates[0].function->insertResultInto(*place, *columns[blocks[0].columns() + i]);
            }
        }

        for (size_t i = 0; i < columns.size(); ++i)
        {
            res.getByPosition(i).column = std::move(columns[i]);
        }

        return res;
    }

    return {};
}


}
