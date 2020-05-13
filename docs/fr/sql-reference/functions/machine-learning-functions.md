---
machine_translated: true
machine_translated_rev: f865c9653f9df092694258e0ccdd733c339112f5
toc_priority: 64
toc_title: Fonctions D'Apprentissage Automatique
---

# Fonctions D’Apprentissage Automatique {#machine-learning-functions}

## evalMLMethod (prédiction) {#machine_learning_methods-evalmlmethod}

Prédiction utilisant des modèles de régression ajustés utilise `evalMLMethod` fonction. Voir le lien dans la `linearRegression`.

### Régression Linéaire Stochastique {#stochastic-linear-regression}

Le [stochasticLinearRegression](../../sql-reference/aggregate-functions/reference.md#agg_functions-stochasticlinearregression) la fonction d’agrégat implémente une méthode de descente de gradient stochastique utilisant un modèle linéaire et une fonction de perte MSE. Utiliser `evalMLMethod` prédire sur de nouvelles données.

### Régression Logistique Stochastique {#stochastic-logistic-regression}

Le [stochasticLogisticRegression](../../sql-reference/aggregate-functions/reference.md#agg_functions-stochasticlogisticregression) la fonction d’agrégation implémente la méthode de descente de gradient stochastique pour le problème de classification binaire. Utiliser `evalMLMethod` prédire sur de nouvelles données.
