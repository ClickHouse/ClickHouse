#pragma once

#include <QueryCoordination/Fragments/StepMatcherBase.h>
#include <QueryCoordination/Fragments/SourceMatcher.h>


namespace DB
{

class createMatcher
{
};

template <typename Matcher, typename T>
Matcher createMatcher(T & node);

}
