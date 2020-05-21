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
