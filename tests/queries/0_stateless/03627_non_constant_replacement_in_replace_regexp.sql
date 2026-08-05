SELECT replaceRegexpAll('1 2 3 123 5 100', '\\d+', number % 2 ? toString(number) : repeat('\\0', number)) FROM numbers(10);
