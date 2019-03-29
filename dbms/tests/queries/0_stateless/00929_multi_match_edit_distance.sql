SET send_logs_level = 'none';

select 0 = multiMatchAnyEditDistance('abc', 0, ['a1c']) from system.numbers limit 5;
select 1 = multiMatchAnyEditDistance('abc', 1, ['a1c']) from system.numbers limit 5;
select 1 = multiMatchAnyEditDistance('abc', 2, ['a1c']) from system.numbers limit 5;
select 1 = multiMatchAnyEditDistance('abc', 3, ['a1c']) from system.numbers limit 5; -- { serverError 49 }
select 1 = multiMatchAnyEditDistance('abc', 4, ['a1c']) from system.numbers limit 5; -- { serverError 49 }

select 1 = multiMatchAnyEditDistance('leftabcright', 1, ['a1c']) from system.numbers limit 5;

select 1 = multiMatchAnyEditDistance('hello some world', 0, ['^hello.*world$']);
select 1 = multiMatchAnyEditDistance('hallo some world', 1, ['^hello.*world$']);
select 0 = multiMatchAnyEditDistance('halo some wrld', 2, ['^hello.*world$']);
select 1 = multiMatchAnyEditDistance('halo some wrld', 2, ['^hello.*world$', '^halo.*world$']);
select 1 = multiMatchAnyEditDistance('halo some wrld', 2, ['^halo.*world$', '^hello.*world$']);
select 1 = multiMatchAnyEditDistance('halo some wrld', 3, ['^hello.*world$']);
select 1 = multiMatchAnyEditDistance('hello some world', 10, ['^hello.*world$']); -- { serverError 49 }
select 1 = multiMatchAnyEditDistance('hello some world', -1, ['^hello.*world$']); -- { serverError 43 }
select 1 = multiMatchAnyEditDistance('hello some world', 10000000000, ['^hello.*world$']); -- { serverError 44 }
select 1 = multiMatchAnyEditDistance('http://hyperscan_is_nice.ru/st', 2, ['http://hyperscan_is_nice.ru/(st\\d\\d$|st\\d\\d\\.|st1[0-4]\\d|st150|st\\d$|gl|rz|ch)']);
select 0 = multiMatchAnyEditDistance('string', 0, ['zorro$', '^tring', 'in$', 'how.*', 'it{2}', 'works']);

select 1 = multiMatchAnyEditDistance('string', 1, ['zorro$', '^tring', 'ip$', 'how.*', 'it{2}', 'works']);
select 2 = multiMatchAnyIndexEditDistance('string', 1, ['zorro$', '^tring', 'ip$', 'how.*', 'it{2}', 'works']);
select 2 = multiMatchAnyIndexEditDistance('halo some wrld', 2, ['^hello.*world$', '^halo.*world$']);
select 1 = multiMatchAnyIndexEditDistance('halo some wrld', 2, ['^halo.*world$', '^hello.*world$']);
