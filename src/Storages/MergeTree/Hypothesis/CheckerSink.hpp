#pragma once
#include <vector>
#include <Processors/Sinks/SinkToStorage.h>
#include "Hypothesis.h"

namespace DB::Hypothesis
{


class CheckerSink : public SinkToStorage
{
public:
    CheckerSink(Block header_, HypothesisVec hypothesis_vec_);

    String getName() const override { return "CheckerSink"; }

    uint64_t getRowsChecked() const {
        return rows_checked;
    }

    HypothesisVec getVerifiedHypothesis() const;

    size_t hypothesisVerifiedCount() const;

protected:
    void consume(Chunk& block) override;


private:
    HypothesisVec hypothesis_vec;
    std::vector<bool> verified; // Guarded by mutex
    uint64_t rows_checked = 0;
    std::mutex mutex;
    LoggerPtr log = nullptr;
};

}
