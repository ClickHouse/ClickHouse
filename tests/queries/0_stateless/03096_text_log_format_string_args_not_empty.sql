set allow_experimental_analyzer = true;

select count; -- { serverError 47 }

select conut(); -- { serverError 46 }

system flush logs;

select count() > 0 from system.text_log where message_format_string = 'Peak memory usage{}: {}.' and value1 is not null and value2 like '% MiB';

select count() > 0 from system.text_log where level = 'Error' and message_format_string = 'Unknown {}{} identifier \'{}\' in scope {}{}' and value1 = 'expression' and value3 = 'count' and value4 = 'SELECT count';

select count() > 0 from system.text_log where level = 'Error' and message_format_string = 'Function with name \'{}\' does not exists. In scope {}{}' and value1 = 'conut' and value2 = 'SELECT conut()' and value3 ilike '%\'count\'%';
