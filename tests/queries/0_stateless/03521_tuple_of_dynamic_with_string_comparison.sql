select tuple('a'::Dynamic) = '(\'a\')';
select tuple(materialize('a')::Dynamic) = '(\'a\')';

