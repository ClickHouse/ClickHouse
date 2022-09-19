#include "GraphiteRender.h"
#include "GraphiteFinder.h"

#include <utility>

namespace DB {

std::string RenderQuery(std::string & table_name_, std::string & target_, int from_, int until_, std::string & format_){
  auto f = new_plain_finder(table_name_);
  std::string query_ = f->generate_query(target_, from_, until_);
  GraphiteRender g(query_, from_, until_, format_);
  return g.generate_render_query();
}

std::string DB::GraphiteRender::generate_render_query() {
  return fmt::format(
      "SELECT Path, groupArray(Time), groupArray(Value), groupArray(Timestamp) FROM graphite_data WHERE {} GROUP BY Path FORMAT {}",
      getRenderWhere(), format);
}
std::string DB::GraphiteRender::getRenderWhere() {
  Where w;
  size_t pos = path.find("FORMAT");
  path = path.substr(0, pos);
  w.And(in_table("Path", path));
  w.And(timestamp_between("Time", from, until));
  return w.string();
}
DB::GraphiteRender::GraphiteRender(std::string & path_, int from_, int until_, std::string & format_)
    : path(path_), from(from_), until(until_), format(format_) {}

}
