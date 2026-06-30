SELECT replaceRegexpOne('line\ntarget', 'line.', 'HI ') = 'HI target';
SELECT replaceRegexpAll('line\ntarget', 'line.', 'HI ') = 'HI target';

SELECT replaceRegexpOne('line\ntarget', '(?-s)line.', 'HI ') = 'line\ntarget';
SELECT replaceRegexpAll('line\ntarget', '(?-s)line.', 'HI ') = 'line\ntarget';

SELECT replaceRegexpOne(arrayJoin(['line\ntarget']), arrayJoin(['line.']), 'HI ') = 'HI target';
SELECT replaceRegexpAll(arrayJoin(['line\ntarget']), arrayJoin(['line.']), materialize('HI ')) = 'HI target';

SELECT replaceRegexpOne('line\ntarget', '(?s)line.', 'HI ') = 'HI target';
SELECT replaceRegexpAll('a\nb c\nd', '.\n.', 'x') = 'x x';

SELECT replaceRegexpOne('line\ntarget', 'line(.)target', 'x\\1') = 'x\n';
SELECT replaceRegexpAll('a\nb\nc', '(.)(\n)', '\\1_') = 'a_b_c';
