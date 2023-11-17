---
slug: /en/operations/keeper-availablity-zone
sidebar_position: 46
sidebar_label: Keeper Availability Zone Aware Load Balancing
---

# Overview
ClickHouse can be configured to prefer a keeper server within the same availability zone. This can help with cross-AZ
network traffic cost. And in some cases, this also improve the load balancing. For example, you have one keeper per
availability zone, and each zone has roughly equal number of ClickHouse replica deployed.

## Configuration Setup
First you need to configure ClickHouse Keeper with its own availability zone. The follow configuration is for a ClickHouse Keeper
deployed in us-west-2b zone.

``` xml
<clickhouse>
    <keeper_server>
        <availability_zone>
            <value>us-west-2b</value>
        </availability_zone>
    </keeper_server>
</clickhouse>
```

You can also configure `availability_zone.enable_auto_detection_on_cloud: 1`. This make Keeper to automatically figure its own
availability zone by checking metadata service. Currently this works on AWS and GCP.

Then configure ClickHouse with availability zone as well.

```xml
<clickhouse>
    <availability_zone>
        <value>us-west-2b</value>
    </availability_zone>
    <zookeeper>
        <zookeeper_load_balancing>keeper_local_availability_zone</zookeeper_load_balancing>
        <fallback_session_lifetime>
            <min>300</min>
            <max>600</max>
        </fallback_session_lifetime>
        <node index="1">
            <host>keeper1</host>
            <port>2181</port>
        </node>
        <node index="2">
            <host>keeper2</host>
            <port>2181</port>
        </node>
        <node index="3">
            <host>keeper3</host>
            <port>2181</port>
        </node>
  </zookeeper>
</clickhouse>
```

When configured as above, ClickHouse would communicate with all three keeper hosts and ask for their availability zone.
ClickHouse would prefer and stay connected with the Keeper host whose availability zone is as same as its own (us-west-b).
In case that keeper host is temporarily unavailable, the session has a fallback lifetime between 5 to 10 minutes.
Once that expiration time reaches, ClickHouse would go through the same process again.

## Caveats
ClickHouse only fallback to other AZ keeper hosts when the it can not find any local AZ keeper available. As long as
one local AZ keeper is available, ClickHouse only consider to connect to that. This potentially can cause overload
on that keeper.