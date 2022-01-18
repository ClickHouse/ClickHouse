select 'no-tld';
select cutToFirstSignificantSubdomainCustom('there-is-no-such-domain', 'public_suffix_list');
-- even if there is no TLD, 2-nd level by default anyway
-- FIXME: make this behavior optional (so that TLD for host never changed, either empty or something real)
select cutToFirstSignificantSubdomainCustom('foo.there-is-no-such-domain', 'public_suffix_list');
select cutToFirstSignificantSubdomainCustom('bar.foo.there-is-no-such-domain', 'public_suffix_list');
select firstSignificantSubdomainCustom('bar.foo.there-is-no-such-domain', 'public_suffix_list');

select 'generic';
select firstSignificantSubdomainCustom('foo.kernel.biz.ss', 'public_suffix_list'); -- kernel.biz.ss
select cutToFirstSignificantSubdomainCustom('foo.kernel.biz.ss', 'public_suffix_list'); -- kernel.biz.ss

select 'difference';
-- biz.ss is not in the default TLD list, hence:
select cutToFirstSignificantSubdomain('foo.kernel.biz.ss'); -- biz.ss
select cutToFirstSignificantSubdomainCustom('foo.kernel.biz.ss', 'public_suffix_list'); -- kernel.biz.ss
