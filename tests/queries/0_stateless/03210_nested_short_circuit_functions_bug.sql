select if(equals(materialize('abc'), 'aws.lambda.duration'), if(toFloat64(materialize('x86_74')) < 50.0000, 0, 1), 0) settings short_circuit_function_evaluation='enable';
select if(equals(materialize('abc'), 'aws.lambda.duration'), if(toFloat64(materialize('x86_74')) < 50.0000, 0, 1), 0) settings short_circuit_function_evaluation='force_enable';

