#pragma once
#include <Processors/ISimpleTransform.h>


namespace DB
{

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

class JoiningTransform : public ISimpleTransform
{
public:
    JoiningTransform(Block input_header, JoinPtr join_,
                     bool on_totals_ = false, bool default_totals_ = false);

    String getName() const override { return "JoiningTransform"; }

    static Block transformHeader(Block header, const JoinPtr & join);

protected:
    void transform(Chunk & chunk) override;
    bool needInputData() const override { return !not_processed; }

private:
    JoinPtr join;
    bool on_totals;
    /// This flag means that we have manually added totals to our pipeline.
    /// It may happen in case if joined subquery has totals, but out string doesn't.
    /// We need to join default values with subquery totals if we have them, or return empty chunk is haven't.
    bool default_totals;
    bool initialized = false;

    ExtraBlockPtr not_processed;

    Block readExecute(Chunk & chunk);
};

}
