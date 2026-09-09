-- A boundary mark of a `Tuple` key column can hold a `NULL` inside the tuple. The key stores NULLs
-- last, while `Field` order puts them below every value, so such a mark used to compare below the mark
-- before it: the granule between them looked empty and the rows it holds were never read.

DROP TABLE IF EXISTS t_pk_tuple_nullable_element;

CREATE TABLE t_pk_tuple_nullable_element (t Tuple(Nullable(Float64), Int32), x Int32) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 4, allow_nullable_key = 1;

INSERT INTO t_pk_tuple_nullable_element VALUES ((1.,1),0),((2.,1),0),((10.,1),1),((500.,7),1),((NULL,2),0),((NULL,3),0),((NULL,4),0),((NULL,5),0);

SELECT 'a NULL in a later granule';
SELECT count() FROM t_pk_tuple_nullable_element WHERE t >= (5., 3);
SELECT t FROM t_pk_tuple_nullable_element WHERE t >= (5., 3) ORDER BY t;
SELECT count() FROM t_pk_tuple_nullable_element WHERE t <= (5., 3);
SELECT count() FROM t_pk_tuple_nullable_element WHERE t = (10., 1);

-- The same counts without the primary key. The condition cache is off here: it is keyed by the
-- condition, so a cached verdict from the queries above would answer these instead of a real read.
SELECT 'without the primary key';
SELECT count() FROM t_pk_tuple_nullable_element WHERE t >= (5., 3) SETTINGS use_primary_key = 0, use_query_condition_cache = 0;
SELECT count() FROM t_pk_tuple_nullable_element WHERE t <= (5., 3) SETTINGS use_primary_key = 0, use_query_condition_cache = 0;

DROP TABLE t_pk_tuple_nullable_element;

SELECT 'a NULL in the first granule';

CREATE TABLE t_pk_tuple_nullable_element (t Tuple(Nullable(Float64), Int32), x Int32) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 2, allow_nullable_key = 1;

INSERT INTO t_pk_tuple_nullable_element VALUES ((1.,1),0),((2.,1),0),((10.,1),1),((500.,7),1),((NULL,2),0),((NULL,3),0);

SELECT count() FROM t_pk_tuple_nullable_element WHERE t >= (2., 1);
SELECT count() FROM t_pk_tuple_nullable_element WHERE t IN ((10., 1), (500., 7));

DROP TABLE t_pk_tuple_nullable_element;
