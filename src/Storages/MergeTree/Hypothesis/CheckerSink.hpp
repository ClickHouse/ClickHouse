#pragma once
#include <vector>
#include <Processors/Sinks/SinkToStorage.h>
#include "Hypothesis.hpp"
#include "Interpreters/Context_fwd.h"

namespace DB::Hypothesis
{

class CheckerSink : public SinkToStorage
{
public:
    CheckerSink(const Block & block_, HypothesisList hypothesis_list_, ContextPtr local_context);

    String getName() const override { return "CheckerSink"; }

    uint64_t getRowsChecked() const { return rows_checked; }

    HypothesisList getVerifiedHypothesis() const;

    size_t hypothesisVerifiedCount() const;

protected:
    void consume(Chunk & chunk) override;


public:
private:
    HypothesisList hypothesis_list;
    std::vector<bool> verified; // Guarded by mutex
    uint64_t rows_checked = 0;
    ContextPtr context;
    mutable std::mutex mutex;
    LoggerPtr log = nullptr;
};

}
