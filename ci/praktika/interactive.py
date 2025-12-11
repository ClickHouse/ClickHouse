"""
Interactive user input utilities for terminal prompts and selections.
"""


class UserPrompt:
    """Provides interactive prompts for user input in terminal."""

    @staticmethod
    def select_from_menu(menuitems, question="Enter your choice"):
        """
        Display a numbered menu and get user's selection.

        Args:
            menuitems: List of items to display. Can be plain values or tuples (display_value, return_value).
            question: The prompt question to display.

        Returns:
            The selected item (or tuple if items are tuples), or None if cancelled.
        """
        menu_map = {}
        for i, item in enumerate(menuitems, start=1):
            menu_map[i] = item
            val = item[0] if isinstance(item, tuple) else item
            print(f"{i}. {val}")

        while True:
            try:
                choice = input(f"\n{question} (1-{len(menuitems)}): ")
                choice_num = int(choice)

                if 1 <= choice_num <= len(menuitems):
                    selected_item = menu_map[choice_num]
                    break
                else:
                    print(
                        f"Invalid choice. Please enter a number between 1 and {len(menuitems)}."
                    )
            except ValueError:
                print("Invalid input. Please enter a number.")

        return selected_item

    @staticmethod
    def get_number(question="Enter a number", validator=lambda x: True):
        """
        Get a numeric input from user with optional validation.

        Args:
            question: The prompt question to display.
            validator: Optional function to validate the number.

        Returns:
            The validated number, or None if cancelled.
        """
        while True:
            try:
                choice = input(f"\n{question}: ")
                choice_num = int(choice)
                if validator(choice_num):
                    break
                else:
                    raise ValueError("Invalid input. Please enter a valid number.")
            except ValueError as e:
                print(f"Invalid input. {e}")
        return choice_num

    @staticmethod
    def confirm(question="Do you want to proceed?"):
        """
        Ask user for yes/no confirmation.

        Args:
            question: The prompt question to display.

        Returns:
            True for yes, False for no, None if cancelled.
        """
        while True:
            choice = input(f"\n{question} (y/n): ")
            if choice.lower() in ("y", "yes"):
                return True
            elif choice.lower() in ("n", "no"):
                return False
            else:
                print("Invalid choice. Please enter 'y' or 'n'.")

    @staticmethod
    def get_string(question, validator=lambda x: True):
        """
        Get a string input from user with optional validation.

        Args:
            question: The prompt question to display.
            validator: Optional function to validate the string.

        Returns:
            The validated string, or None if cancelled.
        """
        while True:
            try:
                choice = input(f"\n{question}: ")
                if validator(choice):
                    break
                else:
                    raise ValueError("Invalid input. Please enter a valid string.")
            except ValueError as e:
                print(f"Invalid input. {e}")
        return choice
