SELECT if(1, 0, throwIf(1, 'Executing FALSE branch'));
SELECT if(empty(''), 0, throwIf(1, 'Executing FALSE branch'));
