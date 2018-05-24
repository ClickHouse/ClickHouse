#pragma once

#include <Processors/IProcessor.h>

namespace DB
{

/** Print pipeline in "dot" format for GraphViz.
  * You can render it with:
  *  dot -T png < pipeline.dot > pipeline.png
  */
void printPipeline(const std::list<ProcessorPtr> &);

}
