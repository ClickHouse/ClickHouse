---
toc_priority: 64
toc_title: "Функции машинного обучения"
---

# Функции машинного обучения {#funktsii-mashinnogo-obucheniia}

## evalMLMethod (prediction) {#machine_learning_methods-evalmlmethod}

Предсказание с использованием подобранных регрессионных моделей.

### Stochastic Linear Regression {#stochastic-linear-regression}

Агрегатная функция [stochasticLinearRegression](../../sql-reference/functions/machine-learning-functions.md#agg_functions-stochasticlinearregression) реализует стохастический градиентный спуск, использую линейную модель и функцию потерь MSE.

### Stochastic Logistic Regression {#stochastic-logistic-regression}

Агрегатная функция [stochasticLogisticRegression](../../sql-reference/functions/machine-learning-functions.md#agg_functions-stochasticlogisticregression) реализует стохастический градиентный спуск для задачи бинарной классификации.

## bayesAB {#bayesab}

Сравнивает тестовые группы (варианты) и для каждой группы рассчитывает вероятность того, что эта группа окажется лучшей. Первая из перечисленных групп считается контрольной.

**Синтаксис** 

``` sql
bayesAB(distribution_name, higher_is_better, variant_names, x, y)
```

**Аргументы** 

-   `distribution_name` — вероятностное распределение. [String](../../sql-reference/data-types/string.md). Возможные значения:

    -   `beta` для [Бета-распределения](https://ru.wikipedia.org/wiki/Бета-распределение)
    -   `gamma` для [Гамма-распределения](https://ru.wikipedia.org/wiki/Гамма-распределение)

-   `higher_is_better` — способ определения предпочтений. [Boolean](../../sql-reference/data-types/boolean.md). Возможные значения:

    -    `0` — чем меньше значение, тем лучше
    -    `1` — чем больше значение, тем лучше

-   `variant_names` — массив, содержащий названия вариантов. [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

-   `x` — массив, содержащий число проведенных тестов (испытаний) для каждого варианта. [Array](../../sql-reference/data-types/array.md)([Float64](../../sql-reference/data-types/float.md)).

-   `y` — массив, содержащий число успешных тестов (испытаний) для каждого варианта. [Array](../../sql-reference/data-types/array.md)([Float64](../../sql-reference/data-types/float.md)).

!!! note "Замечание"
    Все три массива должны иметь одинаковый размер. Все значения `x` и `y` должны быть неотрицательными числами (константами). Значение `y` не может превышать соответствующее значение `x`.

**Возвращаемые значения**

Для каждого варианта рассчитываются:
-   `beats_control` — вероятность, что данный вариант превосходит контрольный в долгосрочной перспективе
-   `to_be_best` — вероятность, что данный вариант является лучшим в долгосрочной перспективе

Тип: JSON.

**Пример**

Запрос:

``` sql
SELECT bayesAB('beta', 1, ['Control', 'A', 'B'], [3000., 3000., 3000.], [100., 90., 110.]) FORMAT PrettySpace;
```

Результат:

``` text
{
   "data":[
      {
         "variant_name":"Control",
         "x":3000,
         "y":100,
         "beats_control":0,
         "to_be_best":0.22619
      },
      {
         "variant_name":"A",
         "x":3000,
         "y":90,
         "beats_control":0.23469,
         "to_be_best":0.04671
      },
      {
         "variant_name":"B",
         "x":3000,
         "y":110,
         "beats_control":0.7580899999999999,
         "to_be_best":0.7271
      }
   ]
}
```
