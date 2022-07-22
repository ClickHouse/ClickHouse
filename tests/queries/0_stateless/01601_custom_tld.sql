-- { echo }

select '-- no-tld';
-- even if there is no TLD, 2-nd level by default anyway
-- FIXME: make this behavior optional (so that TLD for host never changed, either empty or something real)
select cutToFirstSignificantSubdomain('there-is-no-such-domain');
select cutToFirstSignificantSubdomain('foo.there-is-no-such-domain');
select cutToFirstSignificantSubdomain('bar.foo.there-is-no-such-domain');
select cutToFirstSignificantSubdomainCustom('there-is-no-such-domain', 'public_suffix_list');
select cutToFirstSignificantSubdomainCustom('foo.there-is-no-such-domain', 'public_suffix_list');
select cutToFirstSignificantSubdomainCustom('bar.foo.there-is-no-such-domain', 'public_suffix_list');
select firstSignificantSubdomainCustom('bar.foo.there-is-no-such-domain', 'public_suffix_list');

select '-- generic';
select firstSignificantSubdomainCustom('foo.kernel.biz.ss', 'public_suffix_list'); -- kernel
select cutToFirstSignificantSubdomainCustom('foo.kernel.biz.ss', 'public_suffix_list'); -- kernel.biz.ss

select '-- difference';
-- biz.ss is not in the default TLD list, hence:
select cutToFirstSignificantSubdomain('foo.kernel.biz.ss'); -- biz.ss
select cutToFirstSignificantSubdomainCustom('foo.kernel.biz.ss', 'public_suffix_list'); -- kernel.biz.ss

select '-- 3+level';
select cutToFirstSignificantSubdomainCustom('xx.blogspot.co.at', 'public_suffix_list'); -- xx.blogspot.co.at
select firstSignificantSubdomainCustom('xx.blogspot.co.at', 'public_suffix_list'); -- blogspot
select cutToFirstSignificantSubdomainCustom('foo.bar.xx.blogspot.co.at', 'public_suffix_list'); -- xx.blogspot.co.at
select firstSignificantSubdomainCustom('foo.bar.xx.blogspot.co.at', 'public_suffix_list'); -- blogspot

select '-- url';
select cutToFirstSignificantSubdomainCustom('http://foobar.com', 'public_suffix_list');
select cutToFirstSignificantSubdomainCustom('http://foobar.com/foo', 'public_suffix_list');
select cutToFirstSignificantSubdomainCustom('http://bar.foobar.com/foo', 'public_suffix_list');
select cutToFirstSignificantSubdomainCustom('http://xx.blogspot.co.at', 'public_suffix_list');

select '-- www';
select cutToFirstSignificantSubdomainCustomWithWWW('http://www.foo', 'public_suffix_list');
select cutToFirstSignificantSubdomainCustom('http://www.foo', 'public_suffix_list');

select '-- vector';
select cutToFirstSignificantSubdomainCustom('http://xx.blogspot.co.at/' || toString(number), 'public_suffix_list') from numbers(1);
select cutToFirstSignificantSubdomainCustom('there-is-no-such-domain' || toString(number), 'public_suffix_list') from numbers(1);

select '-- no new line';
select cutToFirstSignificantSubdomainCustom('foo.bar', 'no_new_line_list');
select cutToFirstSignificantSubdomainCustom('a.foo.bar', 'no_new_line_list');
select cutToFirstSignificantSubdomainCustom('a.foo.baz', 'no_new_line_list');
