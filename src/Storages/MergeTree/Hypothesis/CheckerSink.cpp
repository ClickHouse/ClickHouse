#include "CheckerSink.hpp"
#include <memory>
#include <mutex>
#include <numeric>
#include "Common/Logger.h"
#include "Common/logger_useful.h"
#include "Storages/MergeTree/Hypothesis/Token.hpp"

namespace DB::Hypothesis
{


CheckerSink::CheckerSink(Block header_, HypothesisVec hypothesis_vec_)
    : SinkToStorage(header_)
    , hypothesis_vec(std::move(hypothesis_vec_))
{
    verified.assign(hypothesis_vec.size(), true);
    log = getLogger("HypothesisChecker");
    LOG_DEBUG(log, "Got {} hypothesis to verify", hypothesis_vec.size());
}

void CheckerSink::consume(Chunk & block)
{
    std::lock_guard guard{mutex};
    const auto & header = input.getHeader();
    rows_checked += block.getNumRows();
    for (size_t i = 0; i < hypothesis_vec.size(); ++i)
    {
        if (!verified[i])
        {
            continue;
        }
        const auto & hypothesis = hypothesis_vec[i];
        bool is_ok = true;
        for (size_t row = 0; row < block.getNumRows() && is_ok; ++row)
        {
            auto deducable_value = block.getColumns()[header.getPositionByName(hypothesis.getName())]->getDataAt(row);
            size_t prefix = 0;
            for (size_t token_idx = 0; token_idx < hypothesis.getSize() && is_ok; ++token_idx)
            {
                auto token_ptr = hypothesis.getToken(token_idx);
                switch (token_ptr->getType())
                {
                    case TokenType::Identity: {
                        auto identity = std::static_pointer_cast<const IdentityToken>(token_ptr);
                        const auto & name = identity->getName();
                        auto pos = header.getPositionByName(name);
                        auto value = block.getColumns()[pos]->getDataAt(row);
                        if (deducable_value.size - prefix < value.size
                            || deducable_value.toView().substr(prefix, value.size) != value.toView())
                        {
                            is_ok = false;
                            break;
                        }
                        prefix += value.size;
                    }
                    break;
                    case TokenType::Const: {
                        auto const_token = std::static_pointer_cast<const ConstToken>(token_ptr);
                        const auto & value = const_token->getValue();
                        if (deducable_value.size - prefix < value.size() || deducable_value.toView().substr(prefix, value.size()) != value)
                        {
                            is_ok = false;
                            break;
                        }
                        prefix += value.size();
                    }
                }
            }
        }
        if (!is_ok)
        {
            verified[i] = false;
        }
    }
}
HypothesisVec CheckerSink::getVerifiedHypothesis() const
{
    HypothesisVec result;
    for (size_t i = 0; i < verified.size(); ++i)
    {
        if (verified[i])
        {
            result.push_back(hypothesis_vec[i]);
        }
    }
    LOG_DEBUG(log, "After verification {} hypothesis left", result.size());
    return result;
}

size_t CheckerSink::hypothesisVerifiedCount() const
{
    return std::accumulate(verified.begin(), verified.end(), 0);
}

}
