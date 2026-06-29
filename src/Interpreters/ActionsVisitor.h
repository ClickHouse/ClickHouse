#pragma once

#include <Interpreters/ActionsMatcher.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

using ActionsVisitor = ConstInDepthNodeVisitor<ActionsMatcher, true>;

}
