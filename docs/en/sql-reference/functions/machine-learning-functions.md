---
description: 'Documentation for Machine Learning Functions'
sidebar_label: 'Machine Learning'
sidebar_position: 115
slug: /sql-reference/functions/machine-learning-functions
title: 'Machine Learning Functions'
---

# Machine Learning Functions

## evalMLMethod {#evalmlmethod}

Prediction using fitted regression models uses `evalMLMethod` function. See link in `linearRegression`.

## stochasticLinearRegression {#stochasticlinearregression}

The [stochasticLinearRegression](/sql-reference/aggregate-functions/reference/stochasticlinearregression) aggregate function implements stochastic gradient descent method using linear model and MSE loss function. Uses `evalMLMethod` to predict on new data.

## stochasticLogisticRegression {#stochasticlogisticregression}

The [stochasticLogisticRegression](/sql-reference/aggregate-functions/reference/stochasticlogisticregression) aggregate function implements stochastic gradient descent method for binary classification problem. Uses `evalMLMethod` to predict on new data.
