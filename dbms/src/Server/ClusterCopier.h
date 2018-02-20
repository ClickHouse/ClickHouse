#pragma once
#include <Poco/Util/ServerApplication.h>

/* = clickhouse-cluster-copier util =
 * Copies tables data from one cluster to new tables of other (possibly the same) cluster in distributed fault-tolerant manner.
 *
 * Configuration of copying tasks is set in special ZooKeeper node (called the description node).
 * A ZooKeeper path to the description node is specified via --task-path </task/path> parameter.
 * So, node /task/path/description should contain special XML content describing copying tasks.
 *
 * Simultaneously many clickhouse-cluster-copier processes located on any servers could execute the same task.
 * ZooKeeper node /task/path/ is used by the processes to coordinate their work.
 * You must not add additional child nodes to /task/path/.
 *
 * Currently you are responsible for launching cluster-copier processes.
 * You can launch as many processes as you want, whenever and wherever you want.
 * Each process try to select nearest available shard of source cluster and copy some part of data (partition) from it to the whole
 * destination cluster with resharding.
 * Therefore it makes sense to launch cluster-copier processes on the source cluster nodes to reduce the network usage.
 *
 * Since the workers coordinate their work via ZooKeeper, in addition to --task-path </task/path> you have to specify ZooKeeper
 * configuration via --config-file <zookeeper.xml> parameter. Example of zookeeper.xml:

   <yandex>
    <zookeeper>
        <node index="1">
            <host>127.0.0.1</host>
            <port>2181</port>
        </node>
    </zookeeper>
   </yandex>

 * When you run clickhouse-cluster-copier --config-file <zookeeper.xml> --task-path </task/path>
 * the process connects to ZooKeeper, reads tasks config from /task/path/description and executes them.
 *
 *
 * = Format of task config =

<yandex>
    <!-- Configuration of clusters as in an ordinary server config -->
    <remote_servers>
        <source_cluster>
            <shard>
                <internal_replication>false</internal_replication>
                    <replica>
                        <host>127.0.0.1</host>
                        <port>9000</port>
                    </replica>
            </shard>
            ...
        </source_cluster>

        <destination_cluster>
        ...
        </destination_cluster>
    </remote_servers>

    <!-- How many simultaneously active workers are possible. If you run more workers superfluous workers will sleep. -->
    <max_workers>2</max_workers>

    <!-- Setting used to fetch (pull) data from source cluster tables -->
    <settings_pull>
        <readonly>1</readonly>
    </settings_pull>

    <!-- Setting used to insert (push) data to destination cluster tables -->
    <settings_push>
        <readonly>0</readonly>
    </settings_push>

    <!-- Common setting for fetch (pull) and insert (push) operations. Also, copier process context uses it.
         They are overlaid by <settings_pull/> and <settings_push/> respectively. -->
    <settings>
        <connect_timeout>3</connect_timeout>
        <!-- Sync insert is set forcibly, leave it here just in case. -->
        <insert_distributed_sync>1</insert_distributed_sync>
    </settings>

    <!-- Copying tasks description.
         You could specify several table task in the same task description (in the same ZooKeeper node), they will be performed
         sequentially.
    -->
    <tables>
        <!-- Name of the table task, it must be an unique name suitable for ZooKeeper node name -->
        <table_hits>
            <-- Source cluster name (from <remote_servers/> section) and tables in it that should be copied -->
            <cluster_pull>source_cluster</cluster_pull>
            <database_pull>test</database_pull>
            <table_pull>hits</table_pull>

            <-- Destination cluster name and tables in which the data should be inserted -->
            <cluster_push>destination_cluster</cluster_push>
            <database_push>test</database_push>
            <table_push>hits2</table_push>

            <!-- Engine of destination tables.
                 If destination tables have not be created, workers create them using columns definition from source tables and engine
                 definition from here.

                 NOTE: If the first worker starts insert data and detects that destination partition is not empty then the partition will
                 be dropped and refilled, take it into account if you already have some data in destination tables. You could directly
                 specify partitions that should be copied in <enabled_partitions/>, they should be in quoted format like partition column of
                 system.parts table.
            -->
            <engine>ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/hits2/{shard}/hits2', '{replica}', EventDate, (CounterID, EventDate), 8192)</engine>

            <!-- Sharding key used to insert data to destination cluster -->
            <sharding_key>intHash32(UserID)</sharding_key>

            <!-- Optional expression that filter data while pull them from source servers -->
            <where_condition>CounterID != 0</where_condition>

            <!-- Optional section, it specifies partitions that should be copied, other partition will be ignored -->
            <enabled_partitions>
                <partition>201712</partition>
                <partition>201801</partition>
                ...
            </enabled_partitions>
        </table_hits>

        </table_visits>
        ...
        </table_visits>
        ...
    </tables>
</yandex>


 * = Implementation details =
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
 *      /table_hits         - directory of table_hits task
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
 *      /test_visits
 *          ...
 */

namespace DB
{

class ClusterCopierApp : public Poco::Util::ServerApplication
{
public:

    void initialize(Poco::Util::Application & self) override;

    void handleHelp(const std::string &, const std::string &);

    void defineOptions(Poco::Util::OptionSet & options) override;

    int main(const std::vector<std::string> &) override;

private:

    void mainImpl();

    void setupLogging();

    std::string config_xml_path;
    std::string task_path;
    std::string log_level = "debug";
    bool is_safe_mode = false;
    double copy_fault_probability = 0;
    bool is_help = false;

    std::string base_dir;
    std::string process_path;
    std::string process_id;
    std::string host_id;
};

}
