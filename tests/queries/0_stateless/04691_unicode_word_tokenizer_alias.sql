-- { echoOn }
-- The `asciiCJK` tokenizer was named `unicode_word` throughout the 26.3 LTS line. That name is persisted in the
-- metadata of tables created there and is re-validated when they are attached, so it has to keep resolving.
select name from system.tokenizers where name in ('asciiCJK', 'unicodeWord', 'unicode_word') order by name;

select tokens('taichi张三丰in the house', 'unicode_word');
select tokens('taichi张三丰in the house', 'unicode_word') = tokens('taichi张三丰in the house', 'asciiCJK');

drop table if exists tab;
create table tab (key UInt64, str String, index text_idx(str) type text(tokenizer = unicode_word)) engine MergeTree order by key;
insert into tab values (1, 'hello错误502需要处理kitty');
explain estimate select * from tab where str like '%错误502需要%';

-- Attaching is the path on which such tables used to fail after an upgrade.
detach table tab;
attach table tab;
select count() from tab;
explain estimate select * from tab where str like '%错误502需要%';

drop table tab;
