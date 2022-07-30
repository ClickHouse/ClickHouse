-- Tags: no-debug, no-fasttest, use-vectorscan

set max_hyperscan_regexp_length = 1;
set max_hyperscan_regexp_total_length = 1;

SELECT '- const pattern';

select multiMatchAny('123', ['1']);
select multiMatchAny('123', ['12']); -- { serverError 36 }
select multiMatchAny('123', ['1', '2']); -- { serverError 36 }

select multiMatchAnyIndex('123', ['1']);
select multiMatchAnyIndex('123', ['12']); -- { serverError 36 }
select multiMatchAnyIndex('123', ['1', '2']); -- { serverError 36 }

select multiMatchAllIndices('123', ['1']);
select multiMatchAllIndices('123', ['12']); -- { serverError 36 }
select multiMatchAllIndices('123', ['1', '2']); -- { serverError 36 }

select multiFuzzyMatchAny('123', 0, ['1']);
select multiFuzzyMatchAny('123', 0, ['12']); -- { serverError 36 }
select multiFuzzyMatchAny('123', 0, ['1', '2']); -- { serverError 36 }

select multiFuzzyMatchAnyIndex('123', 0, ['1']);
select multiFuzzyMatchAnyIndex('123', 0, ['12']); -- { serverError 36 }
select multiFuzzyMatchAnyIndex('123', 0, ['1', '2']); -- { serverError 36 }

select multiFuzzyMatchAllIndices('123', 0, ['1']);
select multiFuzzyMatchAllIndices('123', 0, ['12']); -- { serverError 36 }
select multiFuzzyMatchAllIndices('123', 0, ['1', '2']); -- { serverError 36 }

SELECT '- non-const pattern';

select multiMatchAny(materialize('123'), materialize(['1']));
select multiMatchAny(materialize('123'), materialize(['12'])); -- { serverError 36 }
select multiMatchAny(materialize('123'), materialize(['1', '2'])); -- { serverError 36 }

select multiMatchAnyIndex(materialize('123'), materialize(['1']));
select multiMatchAnyIndex(materialize('123'), materialize(['12'])); -- { serverError 36 }
select multiMatchAnyIndex(materialize('123'), materialize(['1', '2'])); -- { serverError 36 }

select multiMatchAllIndices(materialize('123'), materialize(['1']));
select multiMatchAllIndices(materialize('123'), materialize(['12'])); -- { serverError 36 }
select multiMatchAllIndices(materialize('123'), materialize(['1', '2'])); -- { serverError 36 }

select multiFuzzyMatchAny(materialize('123'), 0, materialize(['1']));
select multiFuzzyMatchAny(materialize('123'), 0, materialize(['12'])); -- { serverError 36 }
select multiFuzzyMatchAny(materialize('123'), 0, materialize(['1', '2'])); -- { serverError 36 }

select multiFuzzyMatchAnyIndex(materialize('123'), 0, materialize(['1']));
select multiFuzzyMatchAnyIndex(materialize('123'), 0, materialize(['12'])); -- { serverError 36 }
select multiFuzzyMatchAnyIndex(materialize('123'), 0, materialize(['1', '2'])); -- { serverError 36 }

select multiFuzzyMatchAllIndices(materialize('123'), 0, materialize(['1']));
select multiFuzzyMatchAllIndices(materialize('123'), 0, materialize(['12'])); -- { serverError 36 }
select multiFuzzyMatchAllIndices(materialize('123'), 0, materialize(['1', '2'])); -- { serverError 36 }
