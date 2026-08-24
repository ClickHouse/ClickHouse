-- test different index type
CREATE TABLE attach_partition_t1 (
	a UInt32,
	b String,
	INDEX bf b TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO attach_partition_t1 SELECT number, toString(number) FROM numbers(10);

CREATE TABLE attach_partition_t2 (
	a UInt32,
	b String,
	INDEX bf b TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY a;

ALTER TABLE attach_partition_t2 ATTACH PARTITION tuple() FROM attach_partition_t1; -- { serverError BAD_ARGUMENTS }

-- test different projection name
CREATE TABLE attach_partition_t3 (
	a UInt32,
	b String,
  PROJECTION proj
   (
       SELECT
           b,
           sum(a)
       GROUP BY b
   )
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO attach_partition_t3 SELECT number, toString(number) FROM numbers(10);

CREATE TABLE attach_partition_t4 (
	a UInt32,
	b String,
  PROJECTION differently_named_proj
   (
       SELECT
           b,
           sum(a)
       GROUP BY b
   )
)
ENGINE = MergeTree
ORDER BY a;

ALTER TABLE attach_partition_t4 ATTACH PARTITION tuple() FROM attach_partition_t3; -- { serverError BAD_ARGUMENTS }

-- check attach with same index and projection
CREATE TABLE attach_partition_t5 (
	a UInt32,
	b String,
  PROJECTION proj
   (
       SELECT
           b,
           sum(a)
       GROUP BY b
   )
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO attach_partition_t5 SELECT number, toString(number) FROM numbers(10);


CREATE TABLE attach_partition_t6 (
	a UInt32,
	b String,
  PROJECTION proj
   (
       SELECT
           b,
           sum(a)
       GROUP BY b
   )
)
ENGINE = MergeTree
ORDER BY a;

ALTER TABLE attach_partition_t6 ATTACH PARTITION tuple() FROM attach_partition_t5;

SELECT * FROM attach_partition_t6 WHERE b = '1';
SELECT b, sum(a) FROM attach_partition_t6 GROUP BY b ORDER BY b;
