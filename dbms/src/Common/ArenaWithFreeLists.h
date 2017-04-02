#pragma once

#include <Common/Arena.h>
#include <Common/BitHelpers.h>


namespace DB
{


/** В отличие от Arena, позволяет освобождать (для последующего повторного использования)
  *  выделенные ранее (не обязательно только что) куски памяти.
  * Для этого, запрашиваемый размер округляется вверх до степени двух
  *  (или до 8, если меньше; или используется выделение памяти вне Arena, если размер больше 65536).
  * При освобождении памяти, для каждого размера (всего 14 вариантов: 8, 16... 65536),
  *  поддерживается односвязный список свободных блоков.
  * При аллокации, мы берём голову списка свободных блоков,
  *  либо, если список пуст - выделяем новый блок, используя Arena.
  */
class ArenaWithFreeLists : private Allocator<false>, private boost::noncopyable
{
private:
    /// Если блок свободен, то в его начале хранится указатель на следующий свободный блок, либо nullptr, если свободных блоков больше нет.
    /// Если блок используется, то в нём хранятся какие-то данные.
    union Block
    {
        Block * next;
        char data[0];
    };

    /// Максимальный размер куска памяти, который выделяется с помощью Arena. Иначе используем Allocator напрямую.
    static constexpr size_t max_fixed_block_size = 65536;

    /// Получить индекс в массиве freelist-ов для заданного размера.
    static size_t findFreeListIndex(const size_t size)
    {
        return size <= 8 ? 2 : bitScanReverse(size - 1);
    }

    /// Для выделения блоков не слишком большого размера используется Arena.
    Arena pool;

    /// Списки свободных блоков. Каждый элемент указывает на голову соответствующего списка, либо равен nullptr.
    /// Первые два элемента не используются, а предназначены для упрощения арифметики.
    Block * free_lists[16] {};

public:
    ArenaWithFreeLists(
        const size_t initial_size = 4096, const size_t growth_factor = 2,
        const size_t linear_growth_threshold = 128 * 1024 * 1024)
        : pool{initial_size, growth_factor, linear_growth_threshold}
    {
    }

    char * alloc(const size_t size)
    {
        if (size > max_fixed_block_size)
            return static_cast<char *>(Allocator::alloc(size));

        /// find list of required size
        const auto list_idx = findFreeListIndex(size);

        /// Если есть свободный блок.
        if (auto & free_block_ptr = free_lists[list_idx])
        {
            /// Возьмём его. И поменяем голову списка на следующий элемент списка.
            const auto res = free_block_ptr->data;
            free_block_ptr = free_block_ptr->next;
            return res;
        }

        /// no block of corresponding size, allocate a new one
        return pool.alloc(1 << (list_idx + 1));
    }

    void free(char * ptr, const size_t size)
    {
        if (size > max_fixed_block_size)
            return Allocator::free(ptr, size);

        /// find list of required size
        const auto list_idx = findFreeListIndex(size);

        /// Вставим освобождённый блок в голову списка.
        auto & free_block_ptr = free_lists[list_idx];
        const auto old_head = free_block_ptr;
        free_block_ptr = reinterpret_cast<Block *>(ptr);
        free_block_ptr->next = old_head;
    }

    /// Размер выделенного пула в байтах
    size_t size() const
    {
        return pool.size();
    }
};


}
