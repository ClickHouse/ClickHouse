---
toc_priority: 222
---

# stochasticLogisticRegression {#agg_functions-stochasticlogisticregression}

Функция реализует стохастическую логистическую регрессию. Её можно использовать для задачи бинарной классификации, функция поддерживает те же пользовательские параметры, что и stochasticLinearRegression и работает таким же образом.

### Параметры {#agg_functions-stochasticlogisticregression-parameters}

Параметры те же, что и в stochasticLinearRegression:
`learning rate`, `l2 regularization coefficient`, `mini-batch size`, `method for updating weights`.
Смотрите раздел [parameters](../../../sql-reference/aggregate-functions/reference/stochasticlinearregression.md#agg_functions-stochasticlinearregression-parameters).

``` text
stochasticLogisticRegression(1.0, 1.0, 10, 'SGD')
```

1.  Построение модели

<!-- -->

Смотрите раздел `Построение модели` в описании [stochasticLinearRegression](../../../sql-reference/aggregate-functions/reference/stochasticlinearregression.md#stochasticlinearregression-usage-fitting) .

    Прогнозируемые метки должны быть в диапазоне \[-1, 1\].

1.  Прогнозирование

<!-- -->

Используя сохраненное состояние, можно предсказать вероятность наличия у объекта метки `1`.

    ``` sql
    WITH (SELECT state FROM your_model) AS model SELECT
    evalMLMethod(model, param1, param2) FROM test_data
    ```

Запрос возвращает столбец вероятностей. Обратите внимание, что первый аргумент `evalMLMethod` это объект `AggregateFunctionState`, далее идут столбцы свойств.

Мы также можем установить границу вероятности, которая присваивает элементам различные метки.

    ``` sql
    SELECT ans < 1.1 AND ans > 0.5 FROM
    (WITH (SELECT state FROM your_model) AS model SELECT
    evalMLMethod(model, param1, param2) AS ans FROM test_data)
    ```

Тогда результатом будут метки.

`test_data` — это таблица, подобная `train_data`, но при этом может не содержать целевое значение.

**Смотрите также**

-   [stochasticLinearRegression](../../../sql-reference/aggregate-functions/reference/stochasticlinearregression.md#agg_functions-stochasticlinearregression)
-   [Отличие линейной от логистической регрессии](https://moredez.ru/q/51225972/)

[Оригинальная статья](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/stochasticlogisticregression/) <!--hide-->
