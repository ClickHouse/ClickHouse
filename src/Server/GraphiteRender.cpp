#include "GraphiteRender.h"
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <utility>
#include "GraphiteFinder.h"
#include <Server/IServer.h>
#include <Poco/Util/LayeredConfiguration.h>

namespace GraphiteCarbon
{

std::string RenderQuery(DB::IServer & server, std::string & table_name_, std::string & target_, int from_, int until_, std::string & format_)
{
    auto f = new_plain_finder(server, table_name_);
    std::string query_ = f->generate_query(target_, from_, until_);
    GraphiteRender g(query_, from_, until_, format_);
    return g.generate_render_query();
}

std::string GraphiteRender::generate_render_query()
{
    return fmt::format(
        "SELECT Path, groupArray(Time), groupArray(Value), groupArray(Timestamp) FROM graphite_data WHERE {} GROUP BY Path FORMAT {}",
        getRenderWhere(),
        format);
}
std::string GraphiteRender::getRenderWhere()
{
    Where w;
    size_t pos = path.find("FORMAT");
    path = path.substr(0, pos);
    w.And(in_table("Path", path));


    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();

    long long seconds_from = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count() + from;
    long long seconds_until = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count() + until;


    w.And(timestamp_between("Time", seconds_from, seconds_until));
    return w.string();
}
GraphiteRender::GraphiteRender(std::string & path_, int from_, int until_, std::string & format_)
    : path(path_), from(from_), until(until_), format(format_)
{
}

}
