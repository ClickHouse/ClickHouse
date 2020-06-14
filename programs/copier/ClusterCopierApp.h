#pragma once

#include <Poco/Util/ServerApplication.h>
#include <daemon/BaseDaemon.h>

#include "ClusterCopier.h"

/* clickhouse cluster copier util
 * Copies tables data from one cluster to new tables of other (possibly the same) cluster in distributed fault-tolerant manner.
 *
 * See overview in the docs: docs/en/utils/clickhouse-copier.md
 *
 * Implementation details:
 *
 * cluster-copier workers pull each partition of each shard of the source cluster and push it to the destination cluster through
 * Distributed table (to preform data resharding). So, worker job is a partition of a source shard.
 * A job has three states: Active, Finished and Abandoned. Abandoned means that worker died and did not finish the job.
 *
 * If an error occurred during the copying (a worker failed or a worker did not finish the INSERT), then the whole partition (on
 * all destination servers) should be dropped and refilled. So, copying entity is a partition of all destination shards.
 * If a failure is detected a special /is_dirty node is created in ZooKeeper signalling that other workers copying the same partition
 * should stop, after a refilling procedure should start.
 *
 * ZooKeeper task node has the following structure:
 *  /task/path_root                     - path passed in --task-path parameter
 *      /description                    - contains user-defined XML config of the task
 *      /task_active_workers            - contains ephemeral nodes of all currently active workers, used to implement max_workers limitation
 *          /server_fqdn#PID_timestamp  - cluster-copier worker ID
 *          ...
 *      /tables             - directory with table tasks
 *      /cluster.db.table1  - directory of table_hits task
 *          /partition1     - directory for partition1
 *              /shards     - directory for source cluster shards
 *                  /1      - worker job for the first shard of partition1 of table test.hits
 *                            Contains info about current status (Active or Finished) and worker ID.
 *                  /2
 *                  ...
 *              /partition_active_workers
 *                  /1      - for each job in /shards a corresponding ephemeral node created in /partition_active_workers
 *                            It is used to detect Abandoned jobs (if there is Active node in /shards and there is no node in
 *                            /partition_active_workers).
 *                            Also, it is used to track active workers in the partition (when we need to refill the partition we do
 *                            not DROP PARTITION while there are active workers)
 *                  /2
 *                  ...
 *              /is_dirty   - the node is set if some worker detected that an error occurred (the INSERT is failed or an Abandoned node is
 *                            detected). If the node appeared workers in this partition should stop and start cleaning and refilling
 *                            partition procedure.
 *                            During this procedure a single 'cleaner' worker is selected. The worker waits for stopping all partition
 *                            workers, removes /shards node, executes DROP PARTITION on each destination node and removes /is_dirty node.
 *                  /cleaner- An ephemeral node used to select 'cleaner' worker. Contains ID of the worker.
 *      /cluster.db.table2
 *          ...
 */

namespace DB
{

class ClusterCopierApp : public BaseDaemon
{
public:

    void initialize(Poco::Util::Application & self) override;

    void handleHelp(const std::string &, const std::string &);

    void defineOptions(Poco::Util::OptionSet & options) override;

    int main(const std::vector<std::string> &) override;

private:

    using Base = BaseDaemon;

    void mainImpl();

    std::string config_xml_path;
    std::string task_path;
    std::string log_level = "trace";
    bool is_safe_mode = false;
    double copy_fault_probability = 0.0;
    double move_fault_probability = 0.0;
    bool is_help = false;

    bool experimental_use_sample_offset{false};

    std::string base_dir;
    std::string process_path;
    std::string process_id;
    std::string host_id;
};

}
