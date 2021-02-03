---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 64
toc_title: "Funciones de aprendizaje autom\xE1tico"
---

# Funciones de aprendizaje automático {#machine-learning-functions}

## evalMLMethod (predicción) {#machine_learning_methods-evalmlmethod}

Predicción utilizando modelos de regresión ajustados utiliza `evalMLMethod` función. Ver enlace en `linearRegression`.

### Regresión lineal estocástica {#stochastic-linear-regression}

El [stochasticLinearRegression](../../sql-reference/aggregate-functions/reference.md#agg_functions-stochasticlinearregression) la función agregada implementa el método de descenso de gradiente estocástico utilizando el modelo lineal y la función de pérdida MSE. Utilizar `evalMLMethod` para predecir sobre nuevos datos.

### Regresión logística estocástica {#stochastic-logistic-regression}

El [stochasticLogisticRegression](../../sql-reference/aggregate-functions/reference.md#agg_functions-stochasticlogisticregression) la función de agregado implementa el método de descenso de gradiente estocástico para el problema de clasificación binaria. Utilizar `evalMLMethod` para predecir sobre nuevos datos.
