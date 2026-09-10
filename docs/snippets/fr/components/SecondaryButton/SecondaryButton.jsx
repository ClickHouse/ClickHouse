export const SecondaryButton = ({ label = 'En savoir plus', href = '#' }) => (
  <a
    href={href}
    className="ch-btn-secondary inline-flex items-center justify-center mt-4 px-5 py-2.5 rounded-md font-semibold text-sm leading-none whitespace-nowrap no-underline hover:no-underline transition-colors bg-transparent border border-black dark:border-[#fdff75] hover:bg-black/5 dark:hover:bg-[#fdff75]/10"
  >
    {label}
  </a>
)