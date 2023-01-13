---
toc_priority: 221
---

# stochasticLinearRegression {#agg_functions-stochasticlinearregression}

Функция реализует стохастическую линейную регрессию. Поддерживает пользовательские параметры для скорости обучения, коэффициента регуляризации L2, размера mini-batch и имеет несколько методов обновления весов ([Adam](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Adam) (по умолчанию), [simple SGD](https://en.wikipedia.org/wiki/Stochastic_gradient_descent), [Momentum](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Momentum), [Nesterov](https://mipt.ru/upload/medialibrary/d7e/41-91.pdf)).

### Параметры {#agg_functions-stochasticlinearregression-parameters}

Есть 4 настраиваемых параметра. Они передаются в функцию последовательно, однако не обязательно указывать все, используются значения по умолчанию, однако хорошая модель требует некоторой настройки параметров.

``` text
stochasticLinearRegression(1.0, 1.0, 10, 'SGD')
```

1.  Скорость обучения — коэффициент длины шага, при выполнении градиентного спуска. Слишком большая скорость обучения может привести к бесконечным весам модели. По умолчанию `0.00001`.
2.  Коэффициент регуляризации l2. Помогает предотвратить подгонку. По умолчанию `0.1`.
3.  Размер mini-batch задаёт количество элементов, чьи градиенты будут вычислены и просуммированы при выполнении одного шага градиентного спуска. Чистый стохастический спуск использует один элемент, однако использование mini-batch (около 10 элементов) делает градиентные шаги более стабильными. По умолчанию `15`.
4.  Метод обновления весов, можно выбрать один из следующих: `Adam` (по умолчанию), `SGD`, `Momentum`, `Nesterov`. `Momentum` и `Nesterov` более требовательные к вычислительным ресурсам и памяти, однако они имеют высокую скорость схождения и устойчивости методов стохастического градиента.

### Использование {#agg_functions-stochasticlinearregression-usage}

`stochasticLinearRegression` используется на двух этапах: построение модели и предсказание новых данных. Чтобы построить модель и сохранить её состояние для дальнейшего использования, мы используем комбинатор `-State`.
Для прогнозирования мы используем функцию [evalMLMethod](../../functions/machine-learning-functions.md#machine_learning_methods-evalmlmethod), которая принимает в качестве аргументов состояние и свойства для прогнозирования.

<a name="stochasticlinearregression-usage-fitting"></a>

**1.** Построение модели

Пример запроса:

``` sql
CREATE TABLE IF NOT EXISTS train_data
(
    param1 Float64,
    param2 Float64,
    target Float64
) ENGINE = Memory;

CREATE TABLE your_model ENGINE = Memory AS SELECT
stochasticLinearRegressionState(0.1, 0.0, 5, 'SGD')(target, param1, param2)
AS state FROM train_data;
```

Здесь нам также нужно вставить данные в таблицу `train_data`. Количество параметров не фиксировано, оно зависит только от количества аргументов, перешедших в `linearRegressionState`. Все они должны быть числовыми значениями.
Обратите внимание, что столбец с целевым значением (которое мы хотели бы научиться предсказывать) вставляется в качестве первого аргумента.

**2.** Прогнозирование

После сохранения состояния в таблице мы можем использовать его несколько раз для прогнозирования или смёржить с другими состояниями и создать новые, улучшенные модели.

```sql
WITH (SELECT state FROM your_model) AS model SELECT
evalMLMethod(model, param1, param2) FROM test_data
```

Запрос возвращает столбец прогнозируемых значений. Обратите внимание, что первый аргумент `evalMLMethod` это объект `AggregateFunctionState`, далее идут столбцы свойств.

`test_data` — это таблица, подобная `train_data`, но при этом может не содержать целевое значение.

### Примечания {#agg_functions-stochasticlinearregression-notes}

1.  Объединить две модели можно следующим запросом:

<!-- -->

```sql
SELECT state1 + state2 FROM your_models
```

где таблица `your_models` содержит обе модели. Запрос вернёт новый объект `AggregateFunctionState`.

1.  Пользователь может получать веса созданной модели для своих целей без сохранения модели, если не использовать комбинатор `-State`.

<!-- -->

```sql
SELECT stochasticLinearRegression(0.01)(target, param1, param2) FROM train_data
```

Подобный запрос строит модель и возвращает её веса, отвечающие параметрам моделей и смещение. Таким образом, в приведенном выше примере запрос вернет столбец с тремя значениями.

**Смотрите также**

-   [stochasticLogisticRegression](../../../sql-reference/aggregate-functions/reference/stochasticlinearregression.md#agg_functions-stochasticlogisticregression)
-   [Отличие линейной от логистической регрессии.](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)

