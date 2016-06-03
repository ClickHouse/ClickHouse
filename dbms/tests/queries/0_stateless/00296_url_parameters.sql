SELECT
    extractURLParameters('http://yandex.ru/?a=b&c=d'),
    extractURLParameters('http://yandex.ru/?a=b&c=d#e=f'),
    extractURLParameters('http://yandex.ru/?a&c=d#e=f'),
    extractURLParameters('http://yandex.ru/?a=b&c=d#e=f&g=h'),
    extractURLParameters('http://yandex.ru/?a=b&c=d#e'),
    extractURLParameters('http://yandex.ru/?a=b&c=d#e&g=h'),
    extractURLParameters('http://yandex.ru/?a=b&c=d#test?e=f&g=h');

SELECT
    extractURLParameterNames('http://yandex.ru/?a=b&c=d'),
    extractURLParameterNames('http://yandex.ru/?a=b&c=d#e=f'),
    extractURLParameterNames('http://yandex.ru/?a&c=d#e=f'),
    extractURLParameterNames('http://yandex.ru/?a=b&c=d#e=f&g=h'),
    extractURLParameterNames('http://yandex.ru/?a=b&c=d#e'),
    extractURLParameterNames('http://yandex.ru/?a=b&c=d#e&g=h'),
    extractURLParameterNames('http://yandex.ru/?a=b&c=d#test?e=f&g=h');

SELECT
    extractURLParameter('http://yandex.ru/?a=b&c=d', 'a'),
    extractURLParameter('http://yandex.ru/?a=b&c=d', 'c'),
    extractURLParameter('http://yandex.ru/?a=b&c=d#e=f', 'e'),
    extractURLParameter('http://yandex.ru/?a&c=d#e=f', 'a'),
    extractURLParameter('http://yandex.ru/?a&c=d#e=f', 'c'),
    extractURLParameter('http://yandex.ru/?a&c=d#e=f', 'e'),
    extractURLParameter('http://yandex.ru/?a=b&c=d#e=f&g=h', 'g'),
    extractURLParameter('http://yandex.ru/?a=b&c=d#e', 'a'),
    extractURLParameter('http://yandex.ru/?a=b&c=d#e', 'c'),
    extractURLParameter('http://yandex.ru/?a=b&c=d#e', 'e'),
    extractURLParameter('http://yandex.ru/?a=b&c=d#e&g=h', 'c'),
    extractURLParameter('http://yandex.ru/?a=b&c=d#e&g=h', 'e'),
    extractURLParameter('http://yandex.ru/?a=b&c=d#e&g=h', 'g'),
    extractURLParameter('http://yandex.ru/?a=b&c=d#test?e=f&g=h', 'test'),
    extractURLParameter('http://yandex.ru/?a=b&c=d#test?e=f&g=h', 'e'),
    extractURLParameter('http://yandex.ru/?a=b&c=d#test?e=f&g=h', 'g');

SELECT
    cutURLParameter('http://yandex.ru/?a=b&c=d', 'a'),
    cutURLParameter('http://yandex.ru/?a=b&c=d', 'c'),
    cutURLParameter('http://yandex.ru/?a=b&c=d#e=f', 'e'),
    cutURLParameter('http://yandex.ru/?a&c=d#e=f', 'a'),
    cutURLParameter('http://yandex.ru/?a&c=d#e=f', 'c'),
    cutURLParameter('http://yandex.ru/?a&c=d#e=f', 'e'),
    cutURLParameter('http://yandex.ru/?a=b&c=d#e=f&g=h', 'g'),
    cutURLParameter('http://yandex.ru/?a=b&c=d#e', 'a'),
    cutURLParameter('http://yandex.ru/?a=b&c=d#e', 'c'),
    cutURLParameter('http://yandex.ru/?a=b&c=d#e', 'e'),
    cutURLParameter('http://yandex.ru/?a=b&c=d#e&g=h', 'c'),
    cutURLParameter('http://yandex.ru/?a=b&c=d#e&g=h', 'e'),
    cutURLParameter('http://yandex.ru/?a=b&c=d#e&g=h', 'g'),
    cutURLParameter('http://yandex.ru/?a=b&c=d#test?e=f&g=h', 'test'),
    cutURLParameter('http://yandex.ru/?a=b&c=d#test?e=f&g=h', 'e'),
    cutURLParameter('http://yandex.ru/?a=b&c=d#test?e=f&g=h', 'g');


SELECT
    extractURLParameters(materialize('http://yandex.ru/?a=b&c=d')),
    extractURLParameters(materialize('http://yandex.ru/?a=b&c=d#e=f')),
    extractURLParameters(materialize('http://yandex.ru/?a&c=d#e=f')),
    extractURLParameters(materialize('http://yandex.ru/?a=b&c=d#e=f&g=h')),
    extractURLParameters(materialize('http://yandex.ru/?a=b&c=d#e')),
    extractURLParameters(materialize('http://yandex.ru/?a=b&c=d#e&g=h')),
    extractURLParameters(materialize('http://yandex.ru/?a=b&c=d#test?e=f&g=h'));

SELECT
    extractURLParameterNames(materialize('http://yandex.ru/?a=b&c=d')),
    extractURLParameterNames(materialize('http://yandex.ru/?a=b&c=d#e=f')),
    extractURLParameterNames(materialize('http://yandex.ru/?a&c=d#e=f')),
    extractURLParameterNames(materialize('http://yandex.ru/?a=b&c=d#e=f&g=h')),
    extractURLParameterNames(materialize('http://yandex.ru/?a=b&c=d#e')),
    extractURLParameterNames(materialize('http://yandex.ru/?a=b&c=d#e&g=h')),
    extractURLParameterNames(materialize('http://yandex.ru/?a=b&c=d#test?e=f&g=h'));

SELECT
    extractURLParameter(materialize('http://yandex.ru/?a=b&c=d'), 'a'),
    extractURLParameter(materialize('http://yandex.ru/?a=b&c=d'), 'c'),
    extractURLParameter(materialize('http://yandex.ru/?a=b&c=d#e=f'), 'e'),
    extractURLParameter(materialize('http://yandex.ru/?a&c=d#e=f'), 'a'),
    extractURLParameter(materialize('http://yandex.ru/?a&c=d#e=f'), 'c'),
    extractURLParameter(materialize('http://yandex.ru/?a&c=d#e=f'), 'e'),
    extractURLParameter(materialize('http://yandex.ru/?a=b&c=d#e=f&g=h'), 'g'),
    extractURLParameter(materialize('http://yandex.ru/?a=b&c=d#e'), 'a'),
    extractURLParameter(materialize('http://yandex.ru/?a=b&c=d#e'), 'c'),
    extractURLParameter(materialize('http://yandex.ru/?a=b&c=d#e'), 'e'),
    extractURLParameter(materialize('http://yandex.ru/?a=b&c=d#e&g=h'), 'c'),
    extractURLParameter(materialize('http://yandex.ru/?a=b&c=d#e&g=h'), 'e'),
    extractURLParameter(materialize('http://yandex.ru/?a=b&c=d#e&g=h'), 'g'),
    extractURLParameter(materialize('http://yandex.ru/?a=b&c=d#test?e=f&g=h'), 'test'),
    extractURLParameter(materialize('http://yandex.ru/?a=b&c=d#test?e=f&g=h'), 'e'),
    extractURLParameter(materialize('http://yandex.ru/?a=b&c=d#test?e=f&g=h'), 'g');

SELECT
    cutURLParameter(materialize('http://yandex.ru/?a=b&c=d'), 'a'),
    cutURLParameter(materialize('http://yandex.ru/?a=b&c=d'), 'c'),
    cutURLParameter(materialize('http://yandex.ru/?a=b&c=d#e=f'), 'e'),
    cutURLParameter(materialize('http://yandex.ru/?a&c=d#e=f'), 'a'),
    cutURLParameter(materialize('http://yandex.ru/?a&c=d#e=f'), 'c'),
    cutURLParameter(materialize('http://yandex.ru/?a&c=d#e=f'), 'e'),
    cutURLParameter(materialize('http://yandex.ru/?a=b&c=d#e=f&g=h'), 'g'),
    cutURLParameter(materialize('http://yandex.ru/?a=b&c=d#e'), 'a'),
    cutURLParameter(materialize('http://yandex.ru/?a=b&c=d#e'), 'c'),
    cutURLParameter(materialize('http://yandex.ru/?a=b&c=d#e'), 'e'),
    cutURLParameter(materialize('http://yandex.ru/?a=b&c=d#e&g=h'), 'c'),
    cutURLParameter(materialize('http://yandex.ru/?a=b&c=d#e&g=h'), 'e'),
    cutURLParameter(materialize('http://yandex.ru/?a=b&c=d#e&g=h'), 'g'),
    cutURLParameter(materialize('http://yandex.ru/?a=b&c=d#test?e=f&g=h'), 'test'),
    cutURLParameter(materialize('http://yandex.ru/?a=b&c=d#test?e=f&g=h'), 'e'),
    cutURLParameter(materialize('http://yandex.ru/?a=b&c=d#test?e=f&g=h'), 'g');
