-- Tags: no-parallel-replicas
with sub as (select number from numbers(1)) select x from (select number as x from remote('127.0.0.{1,2}', numbers(2))) where x in sub settings allow_push_predicate_ast_for_distributed_subqueries = 1, enable_analyzer=1;
select '-';
with sub as (select number from numbers(1)) select x from (select number as x from remote('127.0.0.{1,2}', numbers(2))) where x global in sub settings allow_push_predicate_ast_for_distributed_subqueries = 1, enable_analyzer=1;
