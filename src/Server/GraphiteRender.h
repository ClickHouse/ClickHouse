#pragma once

#include "GraphiteUtils.h"
#include "fmt/format.h"


namespace GraphiteCarbon
{

std::string RenderQuery(std::string & table_name_, std::string & target_, int from_, int until_, std::string & format_);

class GraphiteRender
{
private:
    std::string path;
    int from;
    int until;
    std::string where;
    std::string format;

public:
    GraphiteRender(std::string & path_, int from_, int until_, std::string & format_);

    std::string getRenderWhere();

    std::string generate_render_query();
};

}
