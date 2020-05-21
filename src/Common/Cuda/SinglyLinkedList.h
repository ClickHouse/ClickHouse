#pragma once

template <class T>
class SinglyLinkedList
{
public:
    struct Node
    {
        T data;
        Node * next = nullptr;
    };

    Node * head = nullptr;

public:
    SinglyLinkedList() = default;

    void insert(Node * previous_node, Node * new_node);
    void remove(Node * previous_node, Node * delete_node);
};

template <class T>
void SinglyLinkedList<T>::insert(Node * previous_node, Node * new_node)
{
    if (!previous_node)
    {
        // Is the first node
        new_node->next = head;
        head = new_node;
    }
    else if (!previous_node->next)
    {
        // Is the last node
        previous_node->next = new_node;
        new_node->next = nullptr;
    }
    else
    {
        // Is a middle node
        new_node->next = previous_node->next;
        previous_node->next = new_node;
    }
}

template <class T>
void SinglyLinkedList<T>::remove(Node * previous_node, Node * delete_node)
{
    if (!previous_node)
    {
        head = delete_node->next;
    }
    else
    {
        previous_node->next = delete_node->next;
    }
}
