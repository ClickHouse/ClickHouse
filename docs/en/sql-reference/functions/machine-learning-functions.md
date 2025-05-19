---
description: 'Documentation for Machine Learning Functions'
sidebar_label: 'Machine Learning'
sidebar_position: 115
title: 'Machine Learning Functions'
---

# Machine Learning Functions

## evalMLMethod 

Prediction using fitted regression models uses `evalMLMethod` function. See link in `linearRegression`.

## stochasticLinearRegression 

The [stochasticLinearRegression](/sql-reference/aggregate-functions/reference/stochasticlinearregression) aggregate function implements stochastic gradient descent method using linear model and MSE loss function. Uses `evalMLMethod` to predict on new data.

## stochasticLogisticRegression 

The [stochasticLogisticRegression](/sql-reference/aggregate-functions/reference/stochasticlogisticregression) aggregate function implements stochastic gradient descent method for binary classification problem. Uses `evalMLMethod` to predict on new data.
