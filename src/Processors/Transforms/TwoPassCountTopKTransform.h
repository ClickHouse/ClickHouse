#pragma once

namespace DB
{

class IProcessor;

/// True for the processor of the plan step `TwoPassCountTopK` (optimizeTwoPassCountTopK.cpp). It takes the
/// `rows_before_limit_at_least` counter of the LIMIT below it and adds the rows before the LIMIT of the pass it returns.
bool isTwoPassCountTopKTransform(const IProcessor & processor);

}
