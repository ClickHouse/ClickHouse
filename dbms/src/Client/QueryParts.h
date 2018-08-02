#ifndef CLICKHOUSE_QUERY_PARTS_H
#define CLICKHOUSE_QUERY_PARTS_H

typedef struct {
    const char *name;			/* User printable name of the query part. */
} QUERYPART;

QUERYPART queryParts[] = {
        // CREATE DATABASE, TABLE, VIEW
        {(const char *)"CREATE"},
        {(const char *)"DATABASE"},
        {(const char *)"IF"},
        {(const char *)"NOT"},
        {(const char *)"EXISTS"},
        {(const char *)"TEMPORARY"},
        {(const char *)"TABLE"},
        {(const char *)"ON"},
        {(const char *)"CLUSTER"},
        {(const char *)"DEFAULT"},
        {(const char *)"MATERIALIZED"},
        {(const char *)"ALIAS"},
        {(const char *)"ENGINE"},
        {(const char *)"AS"},
        {(const char *)"VIEW"},
        {(const char *)"POPULATE"},
        {(const char *)"SETTINGS"},
        //ATTACH/DETACH
        {(const char *)"ATTACH"},
        {(const char *)"DETACH"},
        //DROP
        {(const char *)"DROP"},
        //RENAME
        {(const char *)"RENAME"},
        {(const char *)"TO"},
        //ALTER
        {(const char *)"ALTER"},
        {(const char *)"ADD"},
        {(const char *)"MODIFY"},
        {(const char *)"CLEAR"},
        {(const char *)"COLUMN"},
        {(const char *)"AFTER"},
        {(const char *)"COPY"},
        {(const char *)"PROJECT"},
        {(const char *)"PRIMARY"},
        {(const char *)"KEY"},
        //CHECK
        {(const char *)"CHECK"},
        //PARTITIONS
        {(const char *)"PARTITION"},
        {(const char *)"PART"},
        {(const char *)"FREEZE"},
        {(const char *)"FETCH"},
        {(const char *)"FROM"},
        //SHOW DATABASES,TABLES,PROCESSLIST
        {(const char *)"SHOW"},
        {(const char *)"INTO"},
        {(const char *)"OUTFILE"},
        {(const char *)"FORMAT"},
        {(const char *)"TABLES"},
        {(const char *)"DATABASES"},
        {(const char *)"LIKE"},
        {(const char *)"PROCESSLIST"},
        //CONDITIONAL EXPRESSIONS
        {(const char *)"CASE"},
        {(const char *)"WHEN"},
        {(const char *)"THEN"},
        {(const char *)"ELSE"},
        {(const char *)"END"},
        //DESCRIBE
        {(const char *)"DESCRIBE"},
        {(const char *)"DESC"},
        //USE
        {(const char *)"USE"},
        //SET
        {(const char *)"SET"},
        //OPTIMIZE
        {(const char *)"OPTIMIZE"},
        {(const char *)"FINAL"},
        {(const char *)"DEDUPLICATE"},
        //INSERT
        {(const char *)"INSERT"},
        {(const char *)"VALUES"},
        //SELECT
        {(const char *)"SELECT"},
        {(const char *)"DISTINCT"},
        {(const char *)"SAMPLE"},
        {(const char *)"ARRAY"},
        {(const char *)"JOIN"},
        {(const char *)"GLOBAL"},
        {(const char *)"LOCAL"},
        {(const char *)"ANY"},
        {(const char *)"ALL"},
        {(const char *)"INNER"},
        {(const char *)"LEFT"},
        {(const char *)"RIGHT"},
        {(const char *)"FULL"},
        {(const char *)"OUTER"},
        {(const char *)"CROSS"},
        {(const char *)"USING"},
        {(const char *)"PREWHERE"},
        {(const char *)"WHERE"},
        {(const char *)"GROUP"},
        {(const char *)"BY"},
        {(const char *)"WITH"},
        {(const char *)"TOTALS"},
        {(const char *)"HAVING"},
        {(const char *)"ORDER"},
        {(const char *)"COLLATE"},
        {(const char *)"LIMIT"},
        {(const char *)"UNION"},
        {(const char *)"AND"},
        {(const char *)"OR"},
        {(const char *)"ASC"},
        //IN
        {(const char *)"IN"},
        //KILL QUERY
        {(const char *)"KILL"},
        {(const char *)"QUERY"},
        {(const char *)"SYNC"},
        {(const char *)"ASYNC"},
        {(const char *)"TEST"},
        //END OF LIST
        {(const char *)nullptr},
};

#endif //CLICKHOUSE_QUERY_PARTS_H
