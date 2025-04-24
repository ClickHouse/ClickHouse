#pragma once
#include <vector>
#include <Processors/Sinks/SinkToStorage.h>
#include "Hypothesis.hpp"

namespace DB::Hypothesis
{

class CheckerSink : public SinkToStorage
{
public:
    CheckerSink(const Block& block_, HypothesisList hypothesis_list_);

    String getName() const override { return "CheckerSink"; }

    uint64_t getRowsChecked() const { return rows_checked; }

    HypothesisList getVerifiedHypothesis() const;

    size_t hypothesisVerifiedCount() const;

protected:
    void consume(Chunk & block) override;


private:
    HypothesisList hypothesis_list;
    std::vector<bool> verified; // Guarded by mutex
    uint64_t rows_checked = 0;
    std::mutex mutex;
    LoggerPtr log = nullptr;
};

}
