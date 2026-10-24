#pragma once

#include <Processors/ISimpleTransform.h>


namespace DB
{

class ContextTimeSeriesTagsCollector;

/// The transform of `StoreLookedUpTagsStep`: stores the tags of the time series of every chunk into the tags collector and
/// passes on only the columns before `tags_start_index`.
class StoreLookedUpTagsTransform final : public ISimpleTransform
{
public:
    StoreLookedUpTagsTransform(
        const SharedHeader & input_header_,
        const SharedHeader & output_header_,
        size_t tags_start_index_,
        const std::shared_ptr<ContextTimeSeriesTagsCollector> & tags_collector_);

    String getName() const override { return "StoreLookedUpTagsTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    const size_t tags_start_index;
    const std::shared_ptr<ContextTimeSeriesTagsCollector> tags_collector;
};

}
