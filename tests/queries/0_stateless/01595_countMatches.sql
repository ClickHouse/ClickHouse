select 'basic';
select countMatches('', 'foo');
select countMatches('foo', '');
-- simply stop if zero bytes was processed
select countMatches('foo', '[f]{0}');
-- but this is ok
select countMatches('foo', '[f]{0}foo');

select 'case sensitive';
select countMatches('foobarfoo', 'foo');
select countMatches('foobarfoo', 'foo.*');
select countMatches('oooo', 'oo');
select countMatches(concat(toString(number), 'foofoo'), 'foo') from numbers(2);
select countMatches('foobarbazfoobarbaz', 'foo(bar)(?:baz|)');
select countMatches('foo.com bar.com baz.com bam.com', '([^. ]+)\.([^. ]+)');
select countMatches('foo.com@foo.com bar.com@foo.com baz.com@foo.com bam.com@foo.com', '([^. ]+)\.([^. ]+)@([^. ]+)\.([^. ]+)');

select 'case insensitive';
select countMatchesCaseInsensitive('foobarfoo', 'FOo');
select countMatchesCaseInsensitive('foobarfoo', 'FOo.*');
select countMatchesCaseInsensitive('oooo', 'Oo');
select countMatchesCaseInsensitive(concat(toString(number), 'Foofoo'), 'foo') from numbers(2);
select countMatchesCaseInsensitive('foOBarBAZfoobarbaz', 'foo(bar)(?:baz|)');
select countMatchesCaseInsensitive('foo.com BAR.COM baz.com bam.com', '([^. ]+)\.([^. ]+)');
select countMatchesCaseInsensitive('foo.com@foo.com bar.com@foo.com BAZ.com@foo.com bam.com@foo.com', '([^. ]+)\.([^. ]+)@([^. ]+)\.([^. ]+)');

select 'errors';
select countMatches(1, 'foo') from numbers(1); -- { serverError 43; }
select countMatches('foobarfoo', toString(number)) from numbers(1); -- { serverError 44; }
