select 'ipv4';
select port('http://127.0.0.1/');
select port('http://127.0.0.1:80');
select port('http://127.0.0.1:80/');
select port('//127.0.0.1:80/');
select port('127.0.0.1:80');
select 'hostname';
select port('http://foobar.com/');
select port('http://foobar.com:80');
select port('http://foobar.com:80/');
select port('//foobar.com:80/');
select port('foobar.com:80');

select 'default-port';
select port('http://127.0.0.1/', toUInt16(80));
select port('http://foobar.com/', toUInt16(80));

-- unsupported
/* ILLEGAL_TYPE_OF_ARGUMENT */ select port(toFixedString('', 1)); -- { serverError 43; }
/* ILLEGAL_TYPE_OF_ARGUMENT */ select port('', 1); -- { serverError 43; }
/* NUMBER_OF_ARGUMENTS_DOESNT_MATCH */ select port('', 1, 1); -- { serverError 42; }

--
-- Known limitations of domain() (getURLHost())
--
select 'ipv6';
select port('http://[2001:db8::8a2e:370:7334]/');
select port('http://[2001:db8::8a2e:370:7334]:80');
select port('http://[2001:db8::8a2e:370:7334]:80/');
select port('//[2001:db8::8a2e:370:7334]:80/');
select port('[2001:db8::8a2e:370:7334]:80');
select port('2001:db8::8a2e:370:7334:80');
select 'host-no-dot';
select port('//foobar:80/');
