drop table if exists mv_traffic_by_tadig15min;
drop table if exists radacct;

set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE radacct ( radacctid UInt64, f3gppchargingid Nullable(String), f3gppggsnaddress Nullable(String), f3gppggsnmccmnc Nullable(String), f3gppgprsqos Nullable(String), f3gppimeisv Nullable(String), f3gppimsi Nullable(UInt64), f3gppimsimccmnc Nullable(String), f3gpploci Nullable(String), f3gppnsapi Nullable(String), f3gpprattype Nullable(String), f3gppsgsnaddress Nullable(String), f3gppsgsnmccmnc Nullable(String), acctdelaytime Nullable(UInt32), acctinputoctets Nullable(UInt64), acctinputpackets Nullable(UInt64), acctoutputoctets Nullable(UInt64), acctoutputpackets Nullable(UInt64), acctsessionid String, acctstatustype Nullable(String), acctuniqueid String, calledstationid Nullable(String), callingstationid Nullable(String), framedipaddress Nullable(String), nasidentifier Nullable(String), nasipaddress Nullable(String), acctstarttime Nullable(DateTime), acctstoptime Nullable(DateTime), acctsessiontime Nullable(UInt32), acctterminatecause Nullable(String), acctstartdelay Nullable(UInt32), acctstopdelay Nullable(UInt32), connectinfo_start Nullable(String), connectinfo_stop Nullable(String), timestamp DateTime, username Nullable(String), realm Nullable(String), f3gppimsi_int UInt64, f3gppsgsnaddress_int Nullable(UInt32), timestamp_date Date, tac Nullable(String), mnc Nullable(String), tadig LowCardinality(String), country LowCardinality(String), tadig_op_ip Nullable(String) DEFAULT CAST('TADIG NOT FOUND', 'Nullable(String)'), mcc Nullable(UInt16) MATERIALIZED toUInt16OrNull(substring(f3gppsgsnmccmnc, 1, 6))) ENGINE = MergeTree(timestamp_date, (timestamp, radacctid, acctuniqueid), 8192);

insert into radacct values (1, 'a', 'b', 'c', 'd', 'e', 2, 'a', 'b', 'c', 'd', 'e', 'f', 3, 4, 5, 6, 7, 'a', 'Stop', 'c', 'd', 'e', 'f', 'g', 'h', '2018-10-10 15:54:21', '2018-10-10 15:54:21', 8, 'a', 9, 10, 'a', 'b', '2018-10-10 15:54:21', 'a', 'b', 11, 12, '2018-10-10', 'a', 'b', 'c', 'd', 'e');

SELECT any(acctstatustype = 'Stop') FROM radacct WHERE (acctstatustype = 'Stop') AND ((acctinputoctets + acctoutputoctets) > 0);
create materialized view mv_traffic_by_tadig15min Engine=AggregatingMergeTree partition by tadig order by (ts,tadig) populate as select toStartOfFifteenMinutes(timestamp) ts,toDayOfWeek(timestamp) dow, tadig, sumState(acctinputoctets+acctoutputoctets) traffic_bytes,maxState(timestamp) last_stop, minState(radacctid) min_radacctid,maxState(radacctid) max_radacctid from radacct where acctstatustype='Stop' and acctinputoctets+acctoutputoctets > 0 group by tadig,ts,dow;

select tadig, ts, dow, sumMerge(traffic_bytes), maxMerge(last_stop), minMerge(min_radacctid), maxMerge(max_radacctid) from mv_traffic_by_tadig15min group by tadig, ts, dow;

drop table mv_traffic_by_tadig15min;
drop table radacct;
