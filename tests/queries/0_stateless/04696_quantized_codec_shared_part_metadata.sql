-- A part written while a part loaded from disk is in memory must keep the `<column>.quantized`
-- companion stream of the `Quantized` codec: loaded parts rebuild their types from `columns.txt`,
-- without the custom serialization that adds the stream.

SET enable_quantized_codec = 1;

drop table if exists t_quantized_shared_meta;
drop table if exists t_quantized_reference;

create table t_quantized_shared_meta (id UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 64)))
engine = MergeTree order by id
settings min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1; -- both parts must stay separate and alive

insert into t_quantized_shared_meta select number, arrayMap(x -> toFloat32(x + number), range(64)) from numbers(100);

detach table t_quantized_shared_meta;
attach table t_quantized_shared_meta;

insert into t_quantized_shared_meta select number, arrayMap(x -> toFloat32(x + number), range(64)) from numbers(100, 100);

select arrayExists(s -> s like '%quantized%', substreams)
from system.parts_columns
where database = currentDatabase() and table = 't_quantized_shared_meta' and column = 'vec' and name = 'all_2_2_0';

create table t_quantized_reference (id UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 64)))
engine = MergeTree order by id
settings min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

insert into t_quantized_reference select number, arrayMap(x -> toFloat32(x + number), range(64)) from numbers(100, 100);

-- The same rows must quantize to the same codes in both tables. The subcolumn is read inside the
-- subqueries because the old analyzer cannot resolve one through a table alias (`a.vec.quantized`).
select countIf(hex(codes) != hex(reference_codes))
from (select id, vec.quantized as codes from t_quantized_shared_meta) as a
inner join (select id, vec.quantized as reference_codes from t_quantized_reference) as b on a.id = b.id;

select count() from t_quantized_shared_meta;

drop table t_quantized_shared_meta;
drop table t_quantized_reference;
