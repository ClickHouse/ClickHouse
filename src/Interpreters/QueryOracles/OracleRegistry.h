#pragma once

#include <Interpreters/QueryOracles/IOracle.h>

#include <memory>
#include <vector>

namespace DB
{

/// The ordered set of correctness oracles `QueryOracleChecker::check` runs, in dispatch
/// order. Registration is an explicit ordered list in the constructor (one line per
/// oracle) rather than static-initializer magic: order is semantic and documented there,
/// and silencing an oracle is deleting its line.
class OracleRegistry
{
public:
    static const OracleRegistry & instance();

    const std::vector<std::unique_ptr<IOracle>> & oracles() const { return oracle_list; }

private:
    OracleRegistry();

    std::vector<std::unique_ptr<IOracle>> oracle_list;
};

}
