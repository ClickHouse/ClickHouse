SELECT if(number % 2 = 0, [toUUID('00000000-e1fe-11e9-bb8f-853d60c00749')], [toUUID('11111111-e1fe-11e9-bb8f-853d60c00749')]) FROM numbers(5);
