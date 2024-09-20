#include "common/options.h"


std::vector<Option> get_ceph_exporter_options() {
  return std::vector<Option>({
    Option("exporter_sock_dir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("The path to ceph daemons socket files dir")
    .set_default("/var/run/ceph/")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("ceph-exporter"),

    Option("exporter_addr", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Host ip address where exporter is deployed")
    .set_default("0.0.0.0")
    .add_service("ceph-exporter"),

    Option("exporter_http_port", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Port to deploy exporter on. Default is 9926")
    .set_default(9926)
    .add_service("ceph-exporter"),

    Option("exporter_prio_limit", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Only perf counters greater than or equal to exporter_prio_limit are fetched")
    .set_default(5)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("ceph-exporter"),

    Option("exporter_stats_period", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Time to wait before sending requests again to exporter server (seconds)")
    .set_default(5)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("ceph-exporter"),

    Option("exporter_sort_metrics", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("If true it will sort the metrics and group them.")
    .set_default(true)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("ceph-exporter"),


  });
}
