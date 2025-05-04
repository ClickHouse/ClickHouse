select 'query1';

SELECT *
FROM cluster(test_cluster_two_shards, system.one) AS A
GLOBAL INNER JOIN (select * from cluster(test_cluster_two_shards, system.one)) AS B ON A.dummy = B.dummy;

select 'query2';

set use_index_for_in_with_subqueries = 0;

SELECT *
FROM cluster(test_cluster_two_shards, system.one) AS A
GLOBAL INNER JOIN (select * from cluster(test_cluster_two_shards, system.one)) AS B ON A.dummy = B.dummy;
