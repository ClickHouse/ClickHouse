SET send_logs_level = 'none';

select 0 = multiFuzzyMatchAny('abc', 0, ['a1c']) from system.numbers limit 5;
select 1 = multiFuzzyMatchAny('abc', 1, ['a1c']) from system.numbers limit 5;
select 1 = multiFuzzyMatchAny('abc', 2, ['a1c']) from system.numbers limit 5;
select 1 = multiFuzzyMatchAny('abc', 3, ['a1c']) from system.numbers limit 5; -- { serverError 49 }
select 1 = multiFuzzyMatchAny('abc', 4, ['a1c']) from system.numbers limit 5; -- { serverError 49 }

select 1 = multiFuzzyMatchAny('leftabcright', 1, ['a1c']) from system.numbers limit 5;

select 1 = multiFuzzyMatchAny('hello some world', 0, ['^hello.*world$']);
select 1 = multiFuzzyMatchAny('hallo some world', 1, ['^hello.*world$']);
select 0 = multiFuzzyMatchAny('halo some wrld', 2, ['^hello.*world$']);
select 1 = multiFuzzyMatchAny('halo some wrld', 2, ['^hello.*world$', '^halo.*world$']);
select 1 = multiFuzzyMatchAny('halo some wrld', 2, ['^halo.*world$', '^hello.*world$']);
select 1 = multiFuzzyMatchAny('halo some wrld', 3, ['^hello.*world$']);
select 1 = multiFuzzyMatchAny('hello some world', 10, ['^hello.*world$']); -- { serverError 49 }
select 1 = multiFuzzyMatchAny('hello some world', -1, ['^hello.*world$']); -- { serverError 43 }
select 1 = multiFuzzyMatchAny('hello some world', 10000000000, ['^hello.*world$']); -- { serverError 44 }
select 1 = multiFuzzyMatchAny('http://hyperscan_is_nice.ru/st', 2, ['http://hyperscan_is_nice.ru/(st\\d\\d$|st\\d\\d\\.|st1[0-4]\\d|st150|st\\d$|gl|rz|ch)']);
select 0 = multiFuzzyMatchAny('string', 0, ['zorro$', '^tring', 'in$', 'how.*', 'it{2}', 'works']);

select 1 = multiFuzzyMatchAny('string', 1, ['zorro$', '^tring', 'ip$', 'how.*', 'it{2}', 'works']);
select 2 = multiFuzzyMatchAnyIndex('string', 1, ['zorro$', '^tring', 'ip$', 'how.*', 'it{2}', 'works']);
select 2 = multiFuzzyMatchAnyIndex('halo some wrld', 2, ['^hello.*world$', '^halo.*world$']);
select 1 = multiFuzzyMatchAnyIndex('halo some wrld', 2, ['^halo.*world$', '^hello.*world$']);
