-- Tags: no-fasttest, use-vectorscan

SET send_logs_level = 'fatal';

SELECT '- const pattern';

-- run queries multiple times to test the pattern caching
select multiFuzzyMatchAny('abc', 0, ['a1c']) from system.numbers limit 3;
select multiFuzzyMatchAny('abc', 1, ['a1c']) from system.numbers limit 3;
select multiFuzzyMatchAny('abc', 2, ['a1c']) from system.numbers limit 3;
select multiFuzzyMatchAny('abc', 3, ['a1c']) from system.numbers limit 3; -- { serverError 36 }
select multiFuzzyMatchAny('abc', 4, ['a1c']) from system.numbers limit 3; -- { serverError 36 }

select multiFuzzyMatchAny('leftabcright', 1, ['a1c']) from system.numbers limit 3;

select multiFuzzyMatchAny('hello some world', 0, ['^hello.*world$']);
select multiFuzzyMatchAny('hallo some world', 1, ['^hello.*world$']);
select multiFuzzyMatchAny('halo some wrld', 2, ['^hello.*world$']);
select multiFuzzyMatchAny('halo some wrld', 2, ['^hello.*world$', '^halo.*world$']);
select multiFuzzyMatchAny('halo some wrld', 2, ['^halo.*world$', '^hello.*world$']);
select multiFuzzyMatchAny('halo some wrld', 3, ['^hello.*world$']);
select multiFuzzyMatchAny('hello some world', 10, ['^hello.*world$']); -- { serverError 36 }
select multiFuzzyMatchAny('hello some world', -1, ['^hello.*world$']); -- { serverError 43 }
select multiFuzzyMatchAny('hello some world', 10000000000, ['^hello.*world$']); -- { serverError 44 }
select multiFuzzyMatchAny('http://hyperscan_is_nice.de/st', 2, ['http://hyperscan_is_nice.de/(st\\d\\d$|st\\d\\d\\.|st1[0-4]\\d|st150|st\\d$|gl|rz|ch)']);
select multiFuzzyMatchAny('string', 0, ['zorro$', '^tring', 'in$', 'how.*', 'it{2}', 'works']);
select multiFuzzyMatchAny('string', 1, ['zorro$', '^tring', 'ip$', 'how.*', 'it{2}', 'works']);
select multiFuzzyMatchAnyIndex('string', 1, ['zorro$', '^tring', 'ip$', 'how.*', 'it{2}', 'works']);
select multiFuzzyMatchAnyIndex('halo some wrld', 2, ['^hello.*world$', '^halo.*world$']);
select multiFuzzyMatchAnyIndex('halo some wrld', 2, ['^halo.*world$', '^hello.*world$']);
--
select arraySort(multiFuzzyMatchAllIndices('halo some wrld', 2, ['some random string', '^halo.*world$', '^halo.*world$', '^halo.*world$', '^hallllo.*world$']));
select multiFuzzyMatchAllIndices('halo some wrld', 2, ['^halllllo.*world$', 'some random string']);

SELECT '- non-const pattern';

select multiFuzzyMatchAny(materialize('abc'), 0, materialize(['a1c'])) from system.numbers limit 3;
select multiFuzzyMatchAny(materialize('abc'), 1, materialize(['a1c'])) from system.numbers limit 3;
select multiFuzzyMatchAny(materialize('abc'), 2, materialize(['a1c'])) from system.numbers limit 3;
select multiFuzzyMatchAny(materialize('abc'), 3, materialize(['a1c'])) from system.numbers limit 3; -- { serverError 36}
select multiFuzzyMatchAny(materialize('abc'), 4, materialize(['a1c'])) from system.numbers limit 3; -- { serverError 36}

select multiFuzzyMatchAny(materialize('leftabcright'), 1, materialize(['a1c']));

select multiFuzzyMatchAny(materialize('hello some world'), 0, materialize(['^hello.*world$']));
select multiFuzzyMatchAny(materialize('hallo some world'), 1, materialize(['^hello.*world$']));
select multiFuzzyMatchAny(materialize('halo some wrld'), 2, materialize(['^hello.*world$']));
select multiFuzzyMatchAny(materialize('halo some wrld'), 2, materialize(['^hello.*world$', '^halo.*world$']));
select multiFuzzyMatchAny(materialize('halo some wrld'), 2, materialize(['^halo.*world$', '^hello.*world$']));
select multiFuzzyMatchAny(materialize('halo some wrld'), 3, materialize(['^hello.*world$']));
select multiFuzzyMatchAny(materialize('hello some world'), 10, materialize(['^hello.*world$'])); -- { serverError 36 }
select multiFuzzyMatchAny(materialize('hello some world'), -1, materialize(['^hello.*world$'])); -- { serverError 43 }
select multiFuzzyMatchAny(materialize('hello some world'), 10000000000, materialize(['^hello.*world$'])); -- { serverError 44 }
select multiFuzzyMatchAny(materialize('http://hyperscan_is_nice.de/st'), 2, materialize(['http://hyperscan_is_nice.de/(st\\d\\d$|st\\d\\d\\.|st1[0-4]\\d|st150|st\\d$|gl|rz|ch)']));
select multiFuzzyMatchAny(materialize('string'), 0, materialize(['zorro$', '^tring', 'in$', 'how.*', 'it{2}', 'works']));
select multiFuzzyMatchAny(materialize('string'), 1, materialize(['zorro$', '^tring', 'ip$', 'how.*', 'it{2}', 'works']));
select multiFuzzyMatchAnyIndex(materialize('string'), 1, materialize(['zorro$', '^tring', 'ip$', 'how.*', 'it{2}', 'works']));
select multiFuzzyMatchAnyIndex(materialize('halo some wrld'), 2, materialize(['^hello.*world$', '^halo.*world$']));
select multiFuzzyMatchAnyIndex(materialize('halo some wrld'), 2, materialize(['^halo.*world$', '^hello.*world$']));
select arraySort(multiFuzzyMatchAllIndices(materialize('halo some wrld'), 2, materialize(['some random string', '^halo.*world$', '^halo.*world$', '^halo.*world$', '^hallllo.*world$'])));
select multiFuzzyMatchAllIndices(materialize('halo some wrld'), 2, materialize(['^halllllo.*world$', 'some random string']));
