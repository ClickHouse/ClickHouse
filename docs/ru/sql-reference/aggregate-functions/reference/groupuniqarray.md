---
sidebar_position: 111
---

# groupUniqArray {#groupuniqarray}

Синтаксис: `groupUniqArray(x)` или `groupUniqArray(max_size)(x)`

Составляет массив из различных значений аргумента. Расход оперативной памяти такой же, как у функции `uniqExact`.

Функция `groupUniqArray(max_size)(x)` ограничивает размер результирующего массива до `max_size` элементов. Например, `groupUniqArray(1)(x)` равнозначно `[any(x)]`.

