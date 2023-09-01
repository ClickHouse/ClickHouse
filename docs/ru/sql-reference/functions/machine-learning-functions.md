---
sidebar_position: 64
sidebar_label: "Функции машинного обучения"
---

# Функции машинного обучения {#funktsii-mashinnogo-obucheniia}

## evalMLMethod (prediction) {#machine_learning_methods-evalmlmethod}

Предсказание с использованием подобранных регрессионных моделей.

### Stochastic Linear Regression {#stochastic-linear-regression}

Агрегатная функция [stochasticLinearRegression](../../sql-reference/functions/machine-learning-functions.md#agg_functions-stochasticlinearregression) реализует стохастический градиентный спуск, использую линейную модель и функцию потерь MSE.

### Stochastic Logistic Regression {#stochastic-logistic-regression}

Агрегатная функция [stochasticLogisticRegression](../../sql-reference/functions/machine-learning-functions.md#agg_functions-stochasticlogisticregression) реализует стохастический градиентный спуск для задачи бинарной классификации.