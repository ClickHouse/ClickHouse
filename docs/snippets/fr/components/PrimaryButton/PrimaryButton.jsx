export const PrimaryButton = ({ label = 'Commencer', href = '#' }) => (
  <a
    href={href}
    className="ch-btn-primary inline-flex items-center justify-center mt-4 px-5 py-2.5 rounded-md font-semibold text-sm leading-none whitespace-nowrap no-underline hover:no-underline transition-colors bg-black dark:bg-[#fdff75] hover:bg-zinc-800 dark:hover:bg-[#eaec5a]"
  >
    {label}
  </a>
)