#pragma once

#include <Poco/Logger.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/ExpressionAnalyzer.h>    /// SubqueriesForSets


namespace Poco { class Logger; }

namespace DB
{

/** Returns the data from the stream of blocks without changes, but
  * in the `readPrefix` function or before reading the first block
  * initializes all the passed sets.
  */
class CreatingSetsBlockInputStream : public IProfilingBlockInputStream
{
public:
    CreatingSetsBlockInputStream(
        const BlockInputStreamPtr & input,
        const SubqueriesForSets & subqueries_for_sets_,
        const SizeLimits & network_transfer_limits)
        : subqueries_for_sets(subqueries_for_sets_),
        network_transfer_limits(network_transfer_limits)
    {
        for (auto & elem : subqueries_for_sets)
            if (elem.second.source)
                children.push_back(elem.second.source);

        children.push_back(input);
    }

    String getName() const override { return "CreatingSets"; }

    Block getHeader() const override { return children.back()->getHeader(); }

    /// Takes `totals` only from the main source, not from subquery sources.
    Block getTotals() override;

protected:
    Block readImpl() override;
    void readPrefixImpl() override;

private:
    SubqueriesForSets subqueries_for_sets;
    bool created = false;

    SizeLimits network_transfer_limits;

    size_t rows_to_transfer = 0;
    size_t bytes_to_transfer = 0;

    using Logger = Poco::Logger;
    Logger * log = &Logger::get("CreatingSetsBlockInputStream");

    void createAll();
    void createOne(SubqueryForSet & subquery);
};

}
